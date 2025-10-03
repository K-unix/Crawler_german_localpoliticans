import datetime as dt
import io
import json
import logging
import re
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import redis
import sqlalchemy
from pythonjsonlogger import jsonlogger
from sqlalchemy.dialects.postgresql import insert

try:
    from openai import OpenAI  # type: ignore
except ImportError:  # pragma: no cover - available on newer SDKs
    OpenAI = None


class ServiceContextFilter(logging.Filter):
    """Ensure every log record carries the service name for downstream sinks."""

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - logging hook
        if not hasattr(record, "service"):
            record.service = self.service_name
        return True


def configure_logging() -> logging.Logger:
    """Configure root and module specific loggers."""
    log_level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_name, logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        jsonlogger.JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(service)s",
            rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
        )
    )
    handler.addFilter(ServiceContextFilter("db_writer"))

    logging.basicConfig(level=log_level, handlers=[handler])

    logger = logging.getLogger("db_writer")

    redis_log_level_name = os.getenv("REDIS_LOG_LEVEL", log_level_name).upper()
    redis_log_level = getattr(logging, redis_log_level_name, log_level)
    redis_logger = logging.getLogger("redis")
    redis_logger.setLevel(redis_log_level)
    redis_logger.propagate = True

    return logger


LOGGER = configure_logging()

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_QUEUE_NAME = os.getenv("REDIS_QUEUE_NAME", "json_to_db_queue")
REDIS_RETRY_DELAY_SECONDS = float(os.getenv("REDIS_RETRY_DELAY_SECONDS", "5"))
REDIS_BATCH_PENDING_SET = os.getenv("REDIS_BATCH_PENDING_SET", "openai:batch:pending")
REDIS_BATCH_COMPLETED_SET = os.getenv("REDIS_BATCH_COMPLETED_SET", "openai:batch:completed")
REDIS_BATCH_INPUT_SET = os.getenv("REDIS_BATCH_INPUT_SET", "openai:batch:inputs")
REDIS_BATCH_JOB_HASH_PREFIX = os.getenv("REDIS_BATCH_JOB_HASH_PREFIX", "openai:batch:job:")
REDIS_BATCH_REQUEST_METADATA_PREFIX = os.getenv(
    "REDIS_BATCH_REQUEST_METADATA_PREFIX", "openai:batch:request:"
)
REQUEST_METADATA_TTL_SECONDS = int(
    os.getenv("OPENAI_BATCH_METADATA_TTL_SECONDS", str(7 * 24 * 3600))
)

# Queue batching
REDIS_BATCH_SIZE = max(1, int(os.getenv("REDIS_BATCH_SIZE", "5")))

# Database configuration
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/database",
)

# AWS / S3 configuration
AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_ENDPOINT_URL = os.getenv("AWS_ENDPOINT_URL")
S3_CLEANED_BUCKET = (
    os.getenv("S3_CLEANED_BUCKET")
    or os.getenv("S3_DESTINATION_BUCKET")
    or os.getenv("S3_BUCKET")
)
S3_CLEANED_PREFIX = os.getenv("S3_CLEANED_PREFIX", "")
S3_JSONL_BUCKET = os.getenv("S3_JSONL_BUCKET") or S3_CLEANED_BUCKET
S3_JSONL_PREFIX = os.getenv("S3_JSONL_PREFIX", "openai_batches/")
AUTO_DISCOVER_BATCH_INPUTS = os.getenv("OPENAI_BATCH_AUTO_DISCOVER", "1").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
OPENAI_BATCH_INPUT_LIMIT = max(1, int(os.getenv("OPENAI_BATCH_INPUT_LIMIT", "5")))

# Batch processing configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL") or os.getenv("LLM_API_BASE")
OPENAI_BATCH_COMPLETION_WINDOW = os.getenv("OPENAI_BATCH_COMPLETION_WINDOW", "24h")
OPENAI_BATCH_POLL_INTERVAL = max(5.0, float(os.getenv("OPENAI_BATCH_POLL_INTERVAL", "30")))
OPENAI_BATCH_SUBMIT_INTERVAL = max(1.0, float(os.getenv("OPENAI_BATCH_SUBMIT_INTERVAL", "10")))
OPENAI_BATCH_MAX_BYTES = int(os.getenv("OPENAI_BATCH_MAX_BYTES", "5000000"))

# LLM payload expectations
LLM_COLLECTION_KEY = os.getenv("LLM_MEMBERS_KEY", "ratsmitglieder")

# OpenAI client initialisation
openai_client = None
if not OPENAI_API_KEY:
    LOGGER.warning("OPENAI_API_KEY not set; OpenAI batch operations will fail.")
elif OpenAI is None:
    LOGGER.error(
        "openai.OpenAI client unavailable. Please install the OpenAI Python SDK v1.0 or newer."
    )
else:
    client_kwargs: Dict[str, Any] = {"api_key": OPENAI_API_KEY}
    if OPENAI_BASE_URL:
        client_kwargs["base_url"] = OPENAI_BASE_URL
    openai_client = OpenAI(**client_kwargs)

# Create shared clients
LOGGER.info("Initialising database connection ...")
engine = sqlalchemy.create_engine(DATABASE_URL)
metadata = sqlalchemy.MetaData()

try:
    council_members_table = sqlalchemy.Table(
        "council_members", metadata, autoload_with=engine
    )
    LOGGER.info("Loaded table definition for 'council_members'.")
except sqlalchemy.exc.NoSuchTableError:
    LOGGER.critical(
        "Table 'council_members' not found in the database. Please run migrations first."
    )
    sys.exit(1)

LOGGER.info("Connecting to Redis at %s:%s ...", REDIS_HOST, REDIS_PORT)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)

if S3_CLEANED_BUCKET or S3_JSONL_BUCKET:
    boto_session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
        region_name=AWS_REGION or None,
    )
    s3_client = boto_session.client("s3", endpoint_url=AWS_ENDPOINT_URL or None)
else:
    s3_client = None
    LOGGER.warning(
        "No S3 bucket configured (S3_CLEANED_BUCKET/S3_JSONL_BUCKET/S3_DESTINATION_BUCKET)."
    )


@dataclass
class BatchInputTask:
    bucket: str
    key: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RequestContext:
    custom_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def municipality_hint(self) -> Optional[str]:
        return self.metadata.get("municipality") or self.metadata.get("gemeinde")

    def source_file(self) -> Optional[str]:
        source_key = (
            self.metadata.get("source_key")
            or self.metadata.get("cleaned_key")
            or self.metadata.get("source_file")
        )
        if source_key:
            return source_key
        return self.custom_id

    def to_log_context(self) -> Dict[str, Any]:
        ctx = dict(self.metadata)
        ctx.setdefault("custom_id", self.custom_id)
        return ctx


class BatchInputParseError(Exception):
    pass


@dataclass
class BatchInputParseResult:
    endpoint: str
    request_metadata: Dict[str, Dict[str, Any]]
    request_count: int


GERMAN_FIELD_MAP: Dict[str, str] = {
    "gemeinde": "municipality",
    "partei": "party",
    "rollen": "roles",
    "kontaktinformationen": "contact_info",
    "kontaktinfos": "contact_info",
    "notizen": "notes",
    "quelle": "source_file",
}

LEGACY_COLLECTION_KEYS = {"council_members"}


def redis_request_metadata_key(custom_id: str) -> str:
    return f"{REDIS_BATCH_REQUEST_METADATA_PREFIX}{custom_id}"


def redis_job_state_key(job_id: str) -> str:
    return f"{REDIS_BATCH_JOB_HASH_PREFIX}{job_id}"


def store_request_metadata(custom_id: str, metadata: Dict[str, Any]) -> None:
    if not custom_id:
        return
    try:
        redis_client.setex(
            redis_request_metadata_key(custom_id),
            REQUEST_METADATA_TTL_SECONDS,
            json.dumps(metadata, ensure_ascii=False),
        )
    except redis.RedisError:
        LOGGER.exception("Failed to persist request metadata for %s", custom_id)


def load_request_metadata(custom_id: Optional[str]) -> Dict[str, Any]:
    if not custom_id:
        return {}
    try:
        data = redis_client.get(redis_request_metadata_key(custom_id))
    except redis.RedisError:
        LOGGER.exception("Failed to load request metadata for %s", custom_id)
        return {}
    if not data:
        return {}
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        LOGGER.warning("Stored metadata for %s was not valid JSON", custom_id)
        return {}


def store_job_state(job_id: str, updates: Dict[str, Any]) -> None:
    if not job_id:
        return
    mapping: Dict[str, str] = {}
    for key, value in updates.items():
        if value is None:
            continue
        if isinstance(value, (dict, list)):
            mapping[key] = json.dumps(value, ensure_ascii=False)
        else:
            mapping[key] = str(value)
    if not mapping:
        return
    try:
        redis_client.hset(redis_job_state_key(job_id), mapping=mapping)
        redis_client.expire(redis_job_state_key(job_id), REQUEST_METADATA_TTL_SECONDS)
    except redis.RedisError:
        LOGGER.exception("Failed to update job state for %s", job_id)


def load_job_state(job_id: str) -> Dict[str, Any]:
    try:
        raw = redis_client.hgetall(redis_job_state_key(job_id))
    except redis.RedisError:
        LOGGER.exception("Failed to load job state for %s", job_id)
        return {}
    decoded: Dict[str, Any] = {}
    for key, value in raw.items():
        key_str = key.decode("utf-8")
        value_str = value.decode("utf-8")
        try:
            decoded[key_str] = json.loads(value_str)
        except json.JSONDecodeError:
            decoded[key_str] = value_str
    return decoded


def fetch_s3_object(bucket: str, key: str) -> Optional[bytes]:
    if not s3_client:
        LOGGER.error("S3 client not initialised; cannot download s3://%s/%s", bucket, key)
        return None
    try:
        LOGGER.info("Downloading batch input from s3://%s/%s", bucket, key)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()
    except (ClientError, BotoCoreError):
        LOGGER.exception("Failed to download s3://%s/%s", bucket, key)
        return None


def split_batch_content(content: bytes, max_bytes: int) -> List[bytes]:
    if len(content) <= max_bytes:
        return [content]

    segments: List[bytes] = []
    current = bytearray()
    for line in content.splitlines(keepends=True):
        if not line:
            continue
        if len(line) > max_bytes:
            raise BatchInputParseError(
                "Single JSONL line exceeds batch size limit of %s bytes" % max_bytes
            )
        if len(current) + len(line) > max_bytes:
            segments.append(bytes(current))
            current = bytearray()
        current.extend(line)

    if current:
        segments.append(bytes(current))

    return segments


def inspect_batch_input(content: bytes) -> BatchInputParseResult:
    endpoint = None
    request_metadata: Dict[str, Dict[str, Any]] = {}
    request_count = 0

    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.decode("utf-8", errors="replace").strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise BatchInputParseError(
                f"Invalid JSON on line {line_number}: {exc}"
            ) from exc

        request_count += 1
        endpoint = endpoint or record.get("url") or "/v1/chat/completions"
        custom_id = record.get("custom_id")
        metadata = record.get("metadata") or {}
        if custom_id:
            store_request_metadata(custom_id, metadata)
            request_metadata[custom_id] = metadata
        else:
            LOGGER.warning(
                "Batch input line %s missing custom_id; downstream mapping may fail.",
                line_number,
            )

    if endpoint is None:
        raise BatchInputParseError("Batch input did not contain any requests")

    return BatchInputParseResult(endpoint=endpoint, request_metadata=request_metadata, request_count=request_count)


def upload_batch_file(content: bytes) -> Optional[str]:
    if openai_client is None:
        LOGGER.error("OpenAI client not initialised; cannot upload batch file")
        return None
    buffer = io.BytesIO(content)
    buffer.name = "batch.jsonl"
    try:
        response = openai_client.files.create(file=buffer, purpose="batch")
    except Exception:
        LOGGER.exception("Failed to upload batch input file to OpenAI")
        return None
    file_id = getattr(response, "id", None) or response.get("id")
    if not file_id:
        LOGGER.error("File upload response missing id: %s", response)
    return file_id


def create_batch_job(
    file_id: str,
    endpoint: str,
    task: BatchInputTask,
    request_count: int,
    chunk_index: int,
    chunk_total: int,
) -> Optional[str]:
    if openai_client is None:
        return None
    metadata = {
        "input_bucket": task.bucket,
        "input_key": task.key,
        "request_count": request_count,
        "chunk_index": chunk_index,
        "chunk_total": chunk_total,
    }
    try:
        response = openai_client.batches.create(
            input_file_id=file_id,
            endpoint=endpoint,
            completion_window=OPENAI_BATCH_COMPLETION_WINDOW,
            metadata=metadata,
        )
    except Exception:
        LOGGER.exception("Failed to create OpenAI batch job for %s", task.key)
        return None
    job_id = getattr(response, "id", None) or response.get("id")
    if not job_id:
        LOGGER.error("Batch creation response missing id: %s", response)
        return None
    store_job_state(
        job_id,
        {
            "status": getattr(response, "status", None) or response.get("status"),
            "input_bucket": task.bucket,
            "input_key": task.key,
            "input_file_id": file_id,
            "chunk_index": chunk_index,
            "chunk_total": chunk_total,
            "requested_at": dt.datetime.utcnow().isoformat(),
        },
    )
    return job_id


def submit_batch_task(task: BatchInputTask) -> List[str]:
    job_ids: List[str] = []
    if redis_client.sismember(REDIS_BATCH_INPUT_SET, task.key):
        LOGGER.debug("Batch input %s already submitted; skipping", task.key)
        return job_ids
    content = fetch_s3_object(task.bucket, task.key)
    if content is None:
        return job_ids

    try:
        chunks = split_batch_content(content, OPENAI_BATCH_MAX_BYTES)
    except BatchInputParseError:
        LOGGER.exception("Batch input %s contains an oversized record; skipping", task.key)
        return job_ids

    total_chunks = len(chunks)
    for idx, chunk in enumerate(chunks, start=1):
        try:
            parse_result = inspect_batch_input(chunk)
        except BatchInputParseError:
            LOGGER.exception("Batch input %s chunk %s is invalid JSONL; skipping", task.key, idx)
            continue

        file_id = upload_batch_file(chunk)
        if not file_id:
            continue
        job_id = create_batch_job(
            file_id,
            parse_result.endpoint,
            task,
            parse_result.request_count,
            idx,
            total_chunks,
        )
        if not job_id:
            continue

        redis_client.sadd(REDIS_BATCH_PENDING_SET, job_id)
        job_ids.append(job_id)
        LOGGER.info(
            "Submitted OpenAI batch job %s (%s requests, chunk %s/%s) for s3://%s/%s",
            job_id,
            parse_result.request_count,
            idx,
            total_chunks,
            task.bucket,
            task.key,
        )

    if job_ids:
        redis_client.sadd(REDIS_BATCH_INPUT_SET, task.key)

    return job_ids


def discover_new_batch_inputs(limit: int) -> List[BatchInputTask]:
    if not AUTO_DISCOVER_BATCH_INPUTS:
        return []
    if not (S3_JSONL_BUCKET and s3_client):
        return []
    try:
        response = s3_client.list_objects_v2(
            Bucket=S3_JSONL_BUCKET,
            Prefix=S3_JSONL_PREFIX,
        )
    except (ClientError, BotoCoreError):
        LOGGER.exception(
            "Failed to list batch inputs under s3://%s/%s", S3_JSONL_BUCKET, S3_JSONL_PREFIX
        )
        return []
    contents = response.get("Contents", [])
    tasks: List[BatchInputTask] = []
    for obj in contents:
        key = obj.get("Key")
        if not key or key.endswith("/"):
            continue
        if redis_client.sismember(REDIS_BATCH_INPUT_SET, key):
            continue
        tasks.append(
            BatchInputTask(
                bucket=S3_JSONL_BUCKET,
                key=key,
                metadata={"etag": obj.get("ETag"), "size": obj.get("Size")},
            )
        )
        if len(tasks) >= limit:
            break
    return tasks


def extract_json_from_text(text: str) -> Any:
    stripped = text.strip()
    if not stripped:
        raise ValueError("LLM response was empty")

    fence = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)
    match = fence.match(stripped)
    if match:
        stripped = match.group(1).strip()

    for start, end in (("{", "}"), ("[", "]")):
        s_idx = stripped.find(start)
        e_idx = stripped.rfind(end)
        if s_idx != -1 and e_idx != -1 and e_idx > s_idx:
            candidate = stripped[s_idx : e_idx + 1]
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue

    raise ValueError("No valid JSON object found in LLM response")

def ensure_iterable(value: Union[str, List[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [str(value)]


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = re.sub(r"^(Dr\.|Prof\.)\s*", "", name, flags=re.IGNORECASE)
    name = name.lower()
    replacements = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss", " ": "-"}
    for old, new in replacements.items():
        name = name.replace(old, new)
    name = re.sub(r"[^a-z-]", "", name)
    return name


def iter_member_payloads(payload: Any) -> Iterable[Dict[str, Any]]:
    if payload is None:
        return
    if isinstance(payload, dict):
        if LLM_COLLECTION_KEY in payload and isinstance(payload[LLM_COLLECTION_KEY], list):
            base_context = {k: v for k, v in payload.items() if k != LLM_COLLECTION_KEY}
            for entry in payload[LLM_COLLECTION_KEY]:
                if isinstance(entry, dict):
                    merged = {**base_context, **entry}
                    yield merged
        else:
            yield payload
    elif isinstance(payload, list):
        for entry in payload:
            if isinstance(entry, dict):
                yield entry
    else:
        LOGGER.error("Unsupported payload type from LLM: %s", type(payload).__name__)


def translate_german_fields(data: Dict[str, Any]) -> Dict[str, Any]:
    translated: Dict[str, Any] = {}
    for key, value in data.items():
        target_key = GERMAN_FIELD_MAP.get(key, key)
        translated[target_key] = value
    return translated


def prepare_member_record(member: Dict[str, Any], context: Optional[RequestContext]) -> Optional[Dict[str, Any]]:
    raw_municipality = member.get("municipality") or member.get("gemeinde")
    member = translate_german_fields(member)
    full_name = member.get("name")
    municipality = member.get("municipality") or raw_municipality
    if context:
        municipality = municipality or context.municipality_hint()
    if not full_name or not municipality:
        LOGGER.error(
            "Skipping member because 'name' or 'municipality' is missing: %s",
            member,
        )
        return None

    record = dict(member)
    record.setdefault("municipality", municipality)
    if context:
        record.setdefault("source_file", context.source_file())
    record["roles"] = ensure_iterable(record.get("roles"))
    return record


def upsert_council_member(member: Dict[str, Any]) -> None:
    full_name = member.get("name")
    municipality = member.get("municipality")
    if not full_name or not municipality:
        LOGGER.error("Cannot upsert member without name and municipality: %s", member)
        return

    unique_key = f"{municipality.lower()}-{normalize_name(full_name)}"
    values_to_insert = {
        "unique_key": unique_key,
        "full_name": full_name,
        "party": member.get("party"),
        "municipality": municipality,
        "roles": member.get("roles"),
        "source_file": member.get("source_file"),
        "raw_json": member,
    }

    stmt = insert(council_members_table).values(values_to_insert)
    stmt = stmt.on_conflict_do_update(
        index_elements=["unique_key"],
        set_={
            "party": stmt.excluded.party,
            "roles": stmt.excluded.roles,
            "source_file": stmt.excluded.source_file,
            "raw_json": stmt.excluded.raw_json,
            "updated_at": sqlalchemy.func.now(),
        },
    )

    with engine.connect() as connection:
        connection.execute(stmt)
        connection.commit()

    LOGGER.info("Upserted '%s' for municipality '%s'", full_name, municipality)


def process_payload(payload: Any, context: Optional[RequestContext]) -> None:
    origin = context.source_file() if context else "direct message"
    count = 0
    for member in iter_member_payloads(payload):
        record = prepare_member_record(member, context)
        if not record:
            continue
        try:
            upsert_council_member(record)
            count += 1
        except Exception:
            LOGGER.exception("Failed to upsert member: %s", record)
    if count == 0:
        LOGGER.warning("No council members persisted for payload from %s", origin)
    else:
        LOGGER.info("Persisted %s council member(s) for %s", count, origin)


def combine_message_content(content: Any) -> Optional[str]:
    if content is None:
        return None
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: List[str] = []
        for item in content:
            if isinstance(item, dict):
                if item.get("type") == "text" and isinstance(item.get("text"), str):
                    parts.append(item["text"])
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(parts) if parts else None
    return None


def extract_payload_from_batch_record(record: Dict[str, Any], context: RequestContext, job_id: str, line_number: int) -> Optional[Any]:
    response = record.get("response")
    if not response:
        LOGGER.warning(
            "Batch job %s line %s missing response for %s",
            job_id,
            line_number,
            context.custom_id,
        )
        return None

    status_code = response.get("status_code")
    if status_code and int(status_code) >= 300:
        LOGGER.error(
            "Batch job %s line %s returned status %s for %s",
            job_id,
            line_number,
            status_code,
            context.custom_id,
        )
        return None

    body = response.get("body")
    if isinstance(body, str):
        try:
            body = json.loads(body)
        except json.JSONDecodeError:
            LOGGER.exception(
                "Batch job %s line %s had invalid JSON body for %s",
                job_id,
                line_number,
                context.custom_id,
            )
            return None

    if not isinstance(body, dict):
        LOGGER.warning(
            "Batch job %s line %s body type %s unsupported for %s",
            job_id,
            line_number,
            type(body).__name__,
            context.custom_id,
        )
        return None

    if "choices" in body:  # chat/completions style
        choices = body.get("choices") or []
        if not choices:
            LOGGER.warning(
                "Batch job %s line %s contained no choices for %s",
                job_id,
                line_number,
                context.custom_id,
            )
            return None
        message = choices[0].get("message") or {}
        text = combine_message_content(message.get("content"))
        if not text:
            LOGGER.warning(
                "Batch job %s line %s content missing for %s",
                job_id,
                line_number,
                context.custom_id,
            )
            return None
        try:
            return extract_json_from_text(text)
        except ValueError:
            LOGGER.exception(
                "Batch job %s line %s failed JSON extraction for %s",
                job_id,
                line_number,
                context.custom_id,
            )
            return None

    output = body.get("output")
    if isinstance(output, list):  # responses endpoint
        texts: List[str] = []
        for item in output:
            if isinstance(item, dict):
                if item.get("type") == "output_text" and isinstance(item.get("text"), str):
                    texts.append(item["text"])
        if texts:
            text = "\n".join(texts)
            try:
                return extract_json_from_text(text)
            except ValueError:
                LOGGER.exception(
                    "Batch job %s line %s failed JSON extraction for %s",
                    job_id,
                    line_number,
                    context.custom_id,
                )
                return None

    LOGGER.warning(
        "Batch job %s line %s had unsupported body structure for %s",
        job_id,
        line_number,
        context.custom_id,
    )
    return None


def fetch_openai_file_content(file_id: str) -> Optional[bytes]:
    if openai_client is None:
        LOGGER.error("OpenAI client not initialised; cannot fetch file %s", file_id)
        return None
    try:
        response = openai_client.files.content(file_id)
    except Exception:
        LOGGER.exception("Failed to download OpenAI file %s", file_id)
        return None

    for attr in ("read", "getvalue"):
        if hasattr(response, attr):
            try:
                return getattr(response, attr)()
            except Exception:
                continue
    for attr in ("content", "data", "text", "body"):
        value = getattr(response, attr, None)
        if value is None:
            continue
        if isinstance(value, bytes):
            return value
        if isinstance(value, str):
            return value.encode("utf-8")
    if isinstance(response, bytes):
        return response
    LOGGER.warning("Unknown file content type for %s: %s", file_id, type(response).__name__)
    return None


def process_batch_output_file(job_id: str, file_id: str) -> None:
    content = fetch_openai_file_content(file_id)
    if not content:
        LOGGER.error("No content returned for output file %s of job %s", file_id, job_id)
        return

    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.decode("utf-8", errors="replace").strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            LOGGER.exception(
                "Job %s output line %s is invalid JSON", job_id, line_number
            )
            continue

        custom_id = record.get("custom_id") or record.get("id")
        metadata = load_request_metadata(custom_id)
        context = RequestContext(custom_id=custom_id or f"job-{job_id}-line-{line_number}", metadata=metadata)

        error_info = record.get("error")
        if error_info:
            LOGGER.error(
                "Batch job %s line %s failed for %s: %s",
                job_id,
                line_number,
                context.custom_id,
                error_info,
            )
            continue

        payload = extract_payload_from_batch_record(record, context, job_id, line_number)
        if payload is None:
            continue
        process_payload(payload, context)


def process_batch_outputs(job_id: str, output_file_ids: List[str]) -> None:
    if not output_file_ids:
        LOGGER.warning("Batch job %s completed with no output files", job_id)
        return
    for file_id in output_file_ids:
        process_batch_output_file(job_id, file_id)


def poll_pending_batches() -> None:
    if openai_client is None:
        return
    try:
        pending = redis_client.smembers(REDIS_BATCH_PENDING_SET)
    except redis.RedisError:
        LOGGER.exception("Failed to load pending batch job IDs from Redis")
        return
    if not pending:
        return

    for job_id_bytes in pending:
        job_id = job_id_bytes.decode("utf-8")
        try:
            batch = openai_client.batches.retrieve(job_id)
        except Exception:
            LOGGER.exception("Failed to retrieve OpenAI batch job %s", job_id)
            continue

        status = getattr(batch, "status", None) or batch.get("status")
        store_job_state(job_id, {"status": status, "last_checked": dt.datetime.utcnow().isoformat()})

        if status == "completed":
            output_file_ids = list(
                getattr(batch, "output_file_ids", None) or batch.get("output_file_ids") or []
            )
            single_output = getattr(batch, "output_file_id", None)
            if single_output and single_output not in output_file_ids:
                output_file_ids.append(single_output)
            process_batch_outputs(job_id, output_file_ids)
            redis_client.srem(REDIS_BATCH_PENDING_SET, job_id)
            redis_client.sadd(REDIS_BATCH_COMPLETED_SET, job_id)
            store_job_state(job_id, {"completed_at": dt.datetime.utcnow().isoformat()})
            LOGGER.info("Batch job %s completed", job_id)
        elif status in {"failed", "cancelled", "expired"}:
            redis_client.srem(REDIS_BATCH_PENDING_SET, job_id)
            redis_client.sadd(REDIS_BATCH_COMPLETED_SET, job_id)
            store_job_state(job_id, {"finished_at": dt.datetime.utcnow().isoformat()})
            LOGGER.error("Batch job %s ended with status %s", job_id, status)
        else:
            LOGGER.debug("Batch job %s still in status %s", job_id, status)


def submit_new_batches() -> None:
    tasks = discover_new_batch_inputs(OPENAI_BATCH_INPUT_LIMIT)
    for task in tasks:
        submit_batch_task(task)


def parse_queue_message(message: str) -> Any:
    trimmed = message.strip()
    if not trimmed:
        raise ValueError("Empty message from Redis queue")

    try:
        data = json.loads(trimmed)
    except json.JSONDecodeError:
        if trimmed.startswith("s3://"):
            without_scheme = trimmed[5:]
            if "/" in without_scheme:
                bucket, key = without_scheme.split("/", 1)
                return BatchInputTask(bucket=bucket, key=key)
        return trimmed

    if isinstance(data, dict):
        batch_info = None
        if "batch_input" in data and isinstance(data["batch_input"], dict):
            batch_info = data["batch_input"]
        elif any(k in data for k in ("jsonl_key", "batch_key", "input_key")):
            batch_info = data
        if batch_info:
            bucket = (
                batch_info.get("bucket")
                or batch_info.get("jsonl_bucket")
                or S3_JSONL_BUCKET
            )
            key = (
                batch_info.get("jsonl_key")
                or batch_info.get("batch_key")
                or batch_info.get("input_key")
                or batch_info.get("key")
            )
            if bucket and key:
                return BatchInputTask(bucket=bucket, key=key, metadata=batch_info)
        if LLM_COLLECTION_KEY in data or any(k in data for k in ("name", "municipality", "gemeinde")):
            return data
        if "data" in data and isinstance(data["data"], list):
            return data["data"]
        return data

    if isinstance(data, list):
        return data

    return data


def fetch_queue_message(timeout: int) -> Optional[str]:
    if timeout <= 0:
        timeout = 1
    try:
        result = redis_client.brpop(REDIS_QUEUE_NAME, timeout)
    except redis.ConnectionError:
        LOGGER.exception("Lost connection to Redis while blocking for messages")
        time.sleep(REDIS_RETRY_DELAY_SECONDS)
        return None
    if result is None:
        return None
    _, message_bytes = result
    return message_bytes.decode("utf-8")


def drain_queue() -> List[str]:
    messages: List[str] = []
    while True:
        try:
            message_bytes = redis_client.lpop(REDIS_QUEUE_NAME)
        except redis.ConnectionError:
            LOGGER.exception("Lost connection to Redis while draining queue")
            break
        if message_bytes is None:
            break
        messages.append(message_bytes.decode("utf-8"))
    return messages


def process_job(message: str) -> None:
    try:
        parsed = parse_queue_message(message)
    except ValueError:
        LOGGER.exception("Discarding message because it could not be parsed")
        return

    if isinstance(parsed, BatchInputTask):
        submit_batch_task(parsed)
        return

    if isinstance(parsed, dict) or isinstance(parsed, list):
        process_payload(parsed, None)
        return

    LOGGER.warning("Unhandled message type from queue: %s", str(parsed)[:200])


def main_worker_loop() -> None:
    LOGGER.info(
        "Worker started. Listening on Redis queue '%s' and monitoring OpenAI batches...",
        REDIS_QUEUE_NAME,
    )

    next_poll = time.monotonic()
    next_submit = time.monotonic()

    while True:
        try:
            now = time.monotonic()
            if now >= next_submit:
                submit_new_batches()
                next_submit = now + OPENAI_BATCH_SUBMIT_INTERVAL
            if now >= next_poll:
                poll_pending_batches()
                next_poll = now + OPENAI_BATCH_POLL_INTERVAL

            timeout_deadline = min(next_poll, next_submit)
            timeout_seconds = max(1, int(timeout_deadline - time.monotonic()))
            message = fetch_queue_message(timeout_seconds)
            if message:
                process_job(message)
                for extra in drain_queue():
                    process_job(extra)
        except redis.ConnectionError:
            LOGGER.exception(
                "Redis connection error in main loop. Retrying in %s seconds.",
                REDIS_RETRY_DELAY_SECONDS,
            )
            time.sleep(REDIS_RETRY_DELAY_SECONDS)
        except Exception:
            LOGGER.exception("Unexpected error while processing job loop")
            time.sleep(REDIS_RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    main_worker_loop()
