import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Tuple
from urllib.parse import urlparse, urlunparse

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

LOGGER = logging.getLogger("crawler_ingest")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

parsed_db_url = urlparse(DATABASE_URL)
if parsed_db_url.scheme not in {"postgresql", "postgresql+psycopg2", "postgresql+psycopg"}:
    raise RuntimeError(
        "crawler_ingest.py requires a PostgreSQL DATABASE_URL (e.g. postgresql://user:pass@host:5432/db)"
    )
if not parsed_db_url.hostname:
    raise RuntimeError(
        "DATABASE_URL must include a PostgreSQL server hostname or IP; sockets/relative URLs are not supported by crawler_ingest.py"
    )

POSTGRES_HOST_INFO = f"{parsed_db_url.hostname}:{parsed_db_url.port or 5432}/{parsed_db_url.path.lstrip('/')}"

AWS_REGION = os.getenv("AWS_REGION")
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT_URL") or None
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ALLOW_HTTP = os.getenv("AWS_ALLOW_HTTP", "").lower() in {"1", "true", "yes", "on"}

S3_BUCKET = os.getenv("S3_BUCKET")
if not S3_BUCKET:
    raise RuntimeError("S3_BUCKET environment variable is required")

CRAWL_DATA_PREFIX = os.getenv("CRAWL_DATA_PREFIX", "crawl-data/")
CRAWL_HTML_PREFIX = os.getenv("CRAWL_HTML_PREFIX", "crawl-html/")
POLL_INTERVAL = max(5, int(os.getenv("INGEST_POLL_INTERVAL_SECONDS", "30")))

S3_SESSION = boto3.session.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID or None,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
    region_name=AWS_REGION or None,
)
S3_CLIENT = S3_SESSION.client(
    "s3",
    endpoint_url=AWS_ENDPOINT if AWS_ENDPOINT else None,
    verify=not AWS_ALLOW_HTTP,
)


SCHEMA_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS crawled_urls (
        url_id BIGSERIAL PRIMARY KEY,
        url TEXT NOT NULL,
        normalized_url TEXT NOT NULL UNIQUE,
        scheme TEXT,
        host TEXT,
        port INTEGER,
        path TEXT,
        query TEXT,
        first_seen_at TIMESTAMPTZ DEFAULT now(),
        last_seen_at TIMESTAMPTZ,
        last_depth INTEGER
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS storage_objects (
        storage_object_id BIGSERIAL PRIMARY KEY,
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL,
        kind TEXT,
        etag TEXT,
        size_bytes BIGINT,
        content_type TEXT,
        last_modified TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE (bucket, object_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_fetches (
        fetch_id BIGSERIAL PRIMARY KEY,
        url_id BIGINT NOT NULL REFERENCES crawled_urls(url_id),
        storage_object_id BIGINT REFERENCES storage_objects(storage_object_id),
        depth INTEGER,
        scraped_at TIMESTAMPTZ,
        found_links_count INTEGER,
        s3_metadata_bucket TEXT NOT NULL,
        s3_metadata_key TEXT NOT NULL,
        raw_metadata JSONB,
        created_at TIMESTAMPTZ DEFAULT now(),
        updated_at TIMESTAMPTZ DEFAULT now(),
        UNIQUE (s3_metadata_bucket, s3_metadata_key)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS crawl_discovered_links (
        fetch_id BIGINT NOT NULL REFERENCES crawl_fetches(fetch_id) ON DELETE CASCADE,
        position INTEGER NOT NULL,
        target_url_id BIGINT NOT NULL REFERENCES crawled_urls(url_id),
        raw_url TEXT NOT NULL,
        was_new BOOLEAN NOT NULL DEFAULT false,
        created_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (fetch_id, position)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ingest_audit (
        audit_id BIGSERIAL PRIMARY KEY,
        object_key TEXT NOT NULL,
        bucket TEXT NOT NULL,
        kind TEXT NOT NULL,
        processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        status TEXT NOT NULL,
        detail TEXT
    );
    """,
)


def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS rust_crawler"))
        conn.execute(text("SET search_path TO rust_crawler, public"))
        for stmt in SCHEMA_STATEMENTS:
            conn.execute(text(stmt))


def normalize_url(raw_url: str) -> Tuple[str, Dict[str, Optional[str]]]:
    parsed = urlparse(raw_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f"URL missing scheme or host: {raw_url}")

    scheme = parsed.scheme.lower()
    host = parsed.hostname.lower() if parsed.hostname else None
    port = parsed.port

    if scheme in {"http", "https"}:
        if (scheme == "http" and (port is None or port == 80)) or (
            scheme == "https" and (port is None or port == 443)
        ):
            port = None

    netloc = host or ""
    if port:
        netloc = f"{netloc}:{port}"

    path = parsed.path or "/"
    query = parsed.query or ""

    normalized = urlunparse((scheme, netloc, path, "", query, ""))
    return normalized, {
        "scheme": scheme,
        "host": host,
        "port": port,
        "path": path,
        "query": query,
    }


def iso_to_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        cleaned = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        LOGGER.warning("Could not parse timestamp '%s'", value)
        return None


def upsert_url(conn, url: str, depth: Optional[int], seen_at: Optional[datetime]) -> Tuple[int, bool]:
    normalized, parts = normalize_url(url)
    params = {
        "url": url,
        "normalized": normalized,
        "scheme": parts["scheme"],
        "host": parts["host"],
        "port": parts["port"],
        "path": parts["path"],
        "query": parts["query"],
        "last_seen": seen_at or datetime.now(timezone.utc),
        "depth": depth,
    }
    insert_sql = text(
        """
        INSERT INTO rust_crawler.crawled_urls (
            url, normalized_url, scheme, host, port, path, query, first_seen_at, last_seen_at, last_depth
        )
        VALUES (:url, :normalized, :scheme, :host, :port, :path, :query, :last_seen, :last_seen, :depth)
        ON CONFLICT (normalized_url) DO NOTHING
        RETURNING url_id
        """
    )
    row = conn.execute(insert_sql, params).fetchone()
    if row:
        return int(row.url_id), True

    conn.execute(
        text(
            """
            UPDATE rust_crawler.crawled_urls
            SET last_seen_at = GREATEST(last_seen_at, :last_seen),
                last_depth = COALESCE(
                    LEAST(last_depth, :depth),
                    :depth,
                    last_depth
                )
            WHERE normalized_url = :normalized
            """
        ),
        {"normalized": normalized, "last_seen": params["last_seen"], "depth": depth},
    )
    existing = conn.execute(
        text(
            """
            SELECT url_id FROM rust_crawler.crawled_urls WHERE normalized_url = :normalized
            """
        ),
        {"normalized": normalized},
    ).fetchone()
    if not existing:
        raise RuntimeError("Failed to load URL after update")
    return int(existing.url_id), False


def upsert_storage_object(conn, bucket: str, key: str, kind: str, metadata: Dict[str, Optional[str]]) -> Tuple[int, bool]:
    existing = conn.execute(
        text(
            """
            SELECT storage_object_id, etag
            FROM rust_crawler.storage_objects
            WHERE bucket = :bucket AND object_key = :key
            """
        ),
        {"bucket": bucket, "key": key},
    ).fetchone()

    inserted = existing is None
    etag_changed = True
    if existing:
        etag_changed = existing.etag != metadata.get("etag")

    conn.execute(
        text(
            """
            INSERT INTO rust_crawler.storage_objects (
                bucket, object_key, kind, etag, size_bytes, content_type, last_modified, created_at, updated_at
            ) VALUES (
                :bucket, :key, :kind, :etag, :size_bytes, :content_type, :last_modified, now(), now()
            )
            ON CONFLICT (bucket, object_key) DO UPDATE
                SET kind = EXCLUDED.kind,
                    etag = EXCLUDED.etag,
                    size_bytes = EXCLUDED.size_bytes,
                    content_type = EXCLUDED.content_type,
                    last_modified = EXCLUDED.last_modified,
                    updated_at = now()
            """
        ),
        {
            "bucket": bucket,
            "key": key,
            "kind": kind,
            "etag": metadata.get("etag"),
            "size_bytes": metadata.get("size"),
            "content_type": metadata.get("content_type"),
            "last_modified": metadata.get("last_modified"),
        },
    )

    if existing:
        storage_object_id = existing.storage_object_id
    else:
        storage_object_id = conn.execute(
            text(
                """
                SELECT storage_object_id
                FROM rust_crawler.storage_objects
                WHERE bucket = :bucket AND object_key = :key
                """
            ),
            {"bucket": bucket, "key": key},
        ).scalar_one()

    return storage_object_id, inserted or etag_changed


def upsert_crawl_fetch(
    conn,
    url_id: int,
    storage_object_id: int,
    bucket: str,
    key: str,
    depth: Optional[int],
    scraped_at: Optional[datetime],
    found_links: int,
    raw_metadata: Dict,
) -> int:
    result = conn.execute(
        text(
            """
            INSERT INTO rust_crawler.crawl_fetches (
                url_id, storage_object_id, depth, scraped_at, found_links_count,
                s3_metadata_bucket, s3_metadata_key, raw_metadata, created_at, updated_at
            ) VALUES (
                :url_id, :storage_object_id, :depth, :scraped_at, :found_links_count,
                :bucket, :key, :raw_metadata, now(), now()
            )
            ON CONFLICT (s3_metadata_bucket, s3_metadata_key) DO UPDATE
                SET storage_object_id = EXCLUDED.storage_object_id,
                    depth = EXCLUDED.depth,
                    scraped_at = EXCLUDED.scraped_at,
                    found_links_count = EXCLUDED.found_links_count,
                    raw_metadata = EXCLUDED.raw_metadata,
                    updated_at = now()
            RETURNING fetch_id
            """
        ),
        {
            "url_id": url_id,
            "storage_object_id": storage_object_id,
            "depth": depth,
            "scraped_at": scraped_at,
            "found_links_count": found_links,
            "bucket": bucket,
            "key": key,
            "raw_metadata": json.dumps(raw_metadata),
        },
    )
    row = result.fetchone()
    if not row:
        raise RuntimeError("Failed to upsert crawl fetch")
    fetch_id = int(row.fetch_id)
    conn.execute(
        text("DELETE FROM rust_crawler.crawl_discovered_links WHERE fetch_id = :fetch_id"),
        {"fetch_id": fetch_id},
    )
    return fetch_id


def insert_discovered_links(
    conn,
    fetch_id: int,
    links: Iterable[str],
    seen_at: Optional[datetime],
):
    for position, link in enumerate(links):
        try:
            target_id, was_new = upsert_url(conn, link, None, seen_at)
        except ValueError:
            LOGGER.debug("Skipping invalid discovered link: %s", link)
            continue
        conn.execute(
            text(
                """
                INSERT INTO rust_crawler.crawl_discovered_links (
                    fetch_id, position, target_url_id, raw_url, was_new, created_at
                ) VALUES (:fetch_id, :position, :target_url_id, :raw_url, :was_new, now())
                ON CONFLICT (fetch_id, position) DO UPDATE
                    SET target_url_id = EXCLUDED.target_url_id,
                        raw_url = EXCLUDED.raw_url,
                        was_new = EXCLUDED.was_new,
                        created_at = EXCLUDED.created_at
                """
            ),
            {
                "fetch_id": fetch_id,
                "position": position,
                "target_url_id": target_id,
                "raw_url": link,
                "was_new": was_new,
            },
        )


def record_audit(conn, bucket: str, key: str, kind: str, status: str, detail: Optional[str] = None) -> None:
    conn.execute(
        text(
            """
            INSERT INTO rust_crawler.ingest_audit (bucket, object_key, kind, status, detail)
            VALUES (:bucket, :key, :kind, :status, :detail)
            """
        ),
        {"bucket": bucket, "key": key, "kind": kind, "status": status, "detail": detail},
    )


def list_objects(prefix: str) -> Iterable[Dict]:
    continuation: Optional[str] = None
    while True:
        kwargs = {"Bucket": S3_BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        if continuation:
            kwargs["ContinuationToken"] = continuation
        response = S3_CLIENT.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            yield item
        if response.get("IsTruncated"):
            continuation = response.get("NextContinuationToken")
            if not continuation:
                break
        else:
            break


def sync_crawl_metadata(engine: Engine) -> Tuple[int, int]:
    processed = 0
    skipped = 0
    for obj in list_objects(CRAWL_DATA_PREFIX):
        key = obj["Key"]
        etag = obj.get("ETag", "").strip('"')
        last_modified = obj.get("LastModified")
        metadata = {
            "etag": etag,
            "size": obj.get("Size"),
            "content_type": "application/json",
            "last_modified": last_modified,
        }
        try:
            with engine.begin() as conn:
                storage_id, should_process = upsert_storage_object(
                    conn,
                    bucket=S3_BUCKET,
                    key=key,
                    kind="crawl_metadata",
                    metadata=metadata,
                )
                if not should_process:
                    skipped += 1
                    continue

            response = S3_CLIENT.get_object(Bucket=S3_BUCKET, Key=key)
            body = response["Body"].read()
            payload = json.loads(body)
            raw_metadata = payload if isinstance(payload, dict) else {}
            source_url = raw_metadata.get("source_url")
            if not source_url:
                raise ValueError("Crawl metadata missing 'source_url'")
            depth = raw_metadata.get("depth")
            scraped_at = iso_to_datetime(raw_metadata.get("scraped_at"))
            found_links = raw_metadata.get("found_links") or []

            with engine.begin() as conn:
                url_id, _ = upsert_url(conn, source_url, depth, scraped_at)
                storage_id, _ = upsert_storage_object(
                    conn,
                    bucket=S3_BUCKET,
                    key=key,
                    kind="crawl_metadata",
                    metadata=metadata,
                )
                fetch_id = upsert_crawl_fetch(
                    conn,
                    url_id=url_id,
                    storage_object_id=storage_id,
                    bucket=S3_BUCKET,
                    key=key,
                    depth=depth,
                    scraped_at=scraped_at,
                    found_links=len(found_links),
                    raw_metadata=raw_metadata,
                )
                insert_discovered_links(conn, fetch_id, found_links, scraped_at)
                record_audit(conn, S3_BUCKET, key, "crawl_metadata", "processed", None)
            processed += 1
        except (ClientError, BotoCoreError, SQLAlchemyError, ValueError, json.JSONDecodeError) as exc:
            LOGGER.exception("Failed to ingest %s: %s", key, exc)
            with engine.begin() as conn:
                record_audit(conn, S3_BUCKET, key, "crawl_metadata", "error", str(exc))
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("Unexpected error while ingesting %s", key)
            with engine.begin() as conn:
                record_audit(conn, S3_BUCKET, key, "crawl_metadata", "error", str(exc))
    return processed, skipped


def sync_html_objects(engine: Engine) -> int:
    registered = 0
    for obj in list_objects(CRAWL_HTML_PREFIX):
        key = obj["Key"]
        metadata = {
            "etag": obj.get("ETag", "").strip('"'),
            "size": obj.get("Size"),
            "content_type": "text/html",
            "last_modified": obj.get("LastModified"),
        }
        try:
            with engine.begin() as conn:
                _, should_process = upsert_storage_object(
                    conn,
                    bucket=S3_BUCKET,
                    key=key,
                    kind="raw_html",
                    metadata=metadata,
                )
                if should_process:
                    record_audit(conn, S3_BUCKET, key, "raw_html", "tracked", None)
                    registered += 1
        except SQLAlchemyError as exc:
            LOGGER.exception("Database error while tracking HTML object %s", key)
            with engine.begin() as conn:
                record_audit(conn, S3_BUCKET, key, "raw_html", "error", str(exc))
    return registered


def main() -> None:
    engine = create_engine(DATABASE_URL, future=True)
    ensure_schema(engine)
    LOGGER.info(
        "Crawler ingest worker started (postgres=%s bucket=%s data_prefix=%s html_prefix=%s poll_interval=%ss)",
        POSTGRES_HOST_INFO,
        S3_BUCKET,
        CRAWL_DATA_PREFIX,
        CRAWL_HTML_PREFIX,
        POLL_INTERVAL,
    )
    while True:
        start = time.time()
        try:
            processed, skipped = sync_crawl_metadata(engine)
            html_registered = sync_html_objects(engine)
            duration = time.time() - start
            LOGGER.info(
                "Ingest pass complete: processed=%s skipped=%s html_tracked=%s duration=%.2fs",
                processed,
                skipped,
                html_registered,
                duration,
            )
        except KeyboardInterrupt:
            LOGGER.info("Ingest worker shutting down")
            break
        except Exception:  # noqa: BLE001
            LOGGER.exception("Fatal error during ingest pass")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
