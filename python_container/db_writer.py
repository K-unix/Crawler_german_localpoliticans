import json
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, Iterable, List, Optional, Tuple, Union

import boto3
from botocore.exceptions import BotoCoreError, ClientError
import openai
try:
    from openai import OpenAI  # type: ignore
except ImportError:  # pragma: no cover - available on newer SDKs
    OpenAI = None
import redis
import sqlalchemy
from pythonjsonlogger import jsonlogger
from sqlalchemy.dialects.postgresql import insert


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

# LLM configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-5-nano")
LLM_TEMPERATURE = float(os.getenv("LLM_TEMPERATURE", "0"))
LLM_MAX_HTML_CHARS = int(os.getenv("LLM_MAX_HTML_CHARS", "20000"))
LLM_MAX_ATTEMPTS = int(os.getenv("LLM_MAX_ATTEMPTS", "3"))
LLM_RETRY_DELAY_SECONDS = float(os.getenv("LLM_RETRY_DELAY_SECONDS", "3"))
LLM_COLLECTION_KEY = os.getenv("LLM_MEMBERS_KEY", "council_members")
RESPONSES_PER_MINUTE_LIMIT = max(0, int(os.getenv("OPENAI_RESPONSES_PER_MINUTE_LIMIT", "500")))
_tpm_limit_env = os.getenv("OPENAI_TOKENS_PER_MINUTE_LIMIT")
TOKENS_PER_MINUTE_LIMIT = (
    max(0, int(_tpm_limit_env)) if _tpm_limit_env not in {None, ""} else None
)
TOKEN_CHAR_RATIO = max(1, int(os.getenv("OPENAI_TOKEN_CHAR_RATIO", "4")))
DEFAULT_SYSTEM_PROMPT = (
    "You are an information extraction assistant. Extract information about municipal council members from the "
    "provided cleaned HTML and answer strictly in valid JSON. The response must be an object with the key "
    f"'{LLM_COLLECTION_KEY}' containing an array of members. Each member needs the fields: name (string), "
    "municipality (string), party (string or null), roles (array of strings; use an empty array if unknown), "
    "and may include additional keys such as contact_info or notes."
)
LLM_SYSTEM_PROMPT = os.getenv("LLM_SYSTEM_PROMPT", DEFAULT_SYSTEM_PROMPT)
DEFAULT_USER_PROMPT_TEMPLATE = (
    "S3 key: {source_key}\n"
    "Context metadata (JSON): {metadata_json}\n"
    "Extract the council members described in the cleaned HTML below and return only JSON as specified.\n"
    "HTML:\n{html}\n"
)
LLM_USER_PROMPT_TEMPLATE = os.getenv("LLM_USER_PROMPT_TEMPLATE", DEFAULT_USER_PROMPT_TEMPLATE)

openai_client = None
if not OPENAI_API_KEY:
    LOGGER.warning("OPENAI_API_KEY not set; OpenAI provider will fail.")
elif OpenAI is not None:
    base_url = os.getenv("OPENAI_BASE_URL") or os.getenv("LLM_API_BASE")
    client_kwargs: Dict[str, Any] = {"api_key": OPENAI_API_KEY}
    if base_url:
        client_kwargs["base_url"] = base_url
    openai_client = OpenAI(**client_kwargs)
else:
    base_url = (
        os.getenv("OPENAI_API_BASE")
        or os.getenv("OPENAI_BASE_URL")
        or os.getenv("LLM_API_BASE")
    )
    openai.api_key = OPENAI_API_KEY
    if base_url:
        openai.api_base = base_url

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

if S3_CLEANED_BUCKET:
    boto_session = boto3.session.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
        region_name=AWS_REGION or None,
    )
    s3_client = boto_session.client("s3", endpoint_url=AWS_ENDPOINT_URL or None)
else:
    s3_client = None
    LOGGER.warning(
        "No S3 bucket configured (S3_CLEANED_BUCKET/S3_DESTINATION_BUCKET/S3_BUCKET)."
    )


@dataclass
class LLMJob:
    s3_key: str
    bucket: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def resolved_bucket(self) -> Optional[str]:
        return self.bucket or self.metadata.get("bucket") or S3_CLEANED_BUCKET

    def resolved_key(self) -> str:
        key = self.s3_key
        if S3_CLEANED_PREFIX and not key.startswith(S3_CLEANED_PREFIX):
            key = f"{S3_CLEANED_PREFIX.rstrip('/')}/{key.lstrip('/')}"
        return key


class RateLimiter:
    """Simple leaky bucket rate limiter for RPM and TPM caps."""

    def __init__(self, max_calls_per_minute: Optional[int], max_tokens_per_minute: Optional[int]):
        self.max_calls = max_calls_per_minute or None
        self.max_tokens = max_tokens_per_minute or None
        self.call_timestamps: Deque[float] = deque()
        self.token_usage: Deque[Tuple[float, int]] = deque()
        self.tokens_in_window = 0

    def _prune(self, now: float) -> None:
        cutoff = now - 60.0
        while self.call_timestamps and self.call_timestamps[0] <= cutoff:
            self.call_timestamps.popleft()
        while self.token_usage and self.token_usage[0][0] <= cutoff:
            _, tokens = self.token_usage.popleft()
            self.tokens_in_window -= tokens
        if self.tokens_in_window < 0:
            self.tokens_in_window = 0

    def wait_for_slot(self, token_estimate: int) -> None:
        token_estimate = max(1, token_estimate)
        while True:
            now = time.monotonic()
            self._prune(now)
            sleep_for = 0.0
            if self.max_calls and len(self.call_timestamps) >= self.max_calls:
                earliest_call = self.call_timestamps[0]
                sleep_for = max(sleep_for, (earliest_call + 60.0) - now)
            if self.max_tokens and self.tokens_in_window + token_estimate > self.max_tokens and self.token_usage:
                earliest_token_ts = self.token_usage[0][0]
                sleep_for = max(sleep_for, (earliest_token_ts + 60.0) - now)
            if sleep_for <= 0:
                break
            LOGGER.info(
                "Rate limits reached; sleeping %.2f seconds to respect RPM/TPM caps.",
                sleep_for,
            )
            time.sleep(max(sleep_for, 0.0))
        self.call_timestamps.append(time.monotonic())

    def record_tokens(self, tokens: Optional[int]) -> None:
        if tokens is None or tokens <= 0 or self.max_tokens is None:
            return
        now = time.monotonic()
        self._prune(now)
        token_value = int(tokens)
        self.token_usage.append((now, token_value))
        self.tokens_in_window += token_value


rate_limiter = RateLimiter(
    RESPONSES_PER_MINUTE_LIMIT if RESPONSES_PER_MINUTE_LIMIT > 0 else None,
    TOKENS_PER_MINUTE_LIMIT,
)


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


def parse_queue_message(message: str) -> Union[LLMJob, Dict[str, Any], List[Any]]:
    trimmed = message.strip()
    if not trimmed:
        raise ValueError("Empty message from Redis queue")

    try:
        data = json.loads(trimmed)
    except json.JSONDecodeError:
        return LLMJob(s3_key=trimmed)

    if isinstance(data, dict):
        if "name" in data and "municipality" in data:
            return data
        if LLM_COLLECTION_KEY in data:
            return data
        candidate_key = (
            data.get("s3_key")
            or data.get("key")
            or data.get("cleaned_key")
            or data.get("source_key")
        )
        if candidate_key:
            metadata = data.get("metadata")
            if not isinstance(metadata, dict):
                metadata = {
                    k: v
                    for k, v in data.items()
                    if k not in {"s3_key", "bucket", "key", "cleaned_key", "source_key"}
                }
            return LLMJob(
                s3_key=candidate_key,
                bucket=data.get("bucket"),
                metadata=metadata or {},
            )
        return data

    if isinstance(data, list):
        return {LLM_COLLECTION_KEY: data}

    raise ValueError(f"Unsupported message format: {type(data)!r}")


def download_cleaned_html(job: LLMJob) -> Optional[str]:
    bucket = job.resolved_bucket()
    if not bucket:
        LOGGER.error("No bucket specified for job %s", job.s3_key)
        return None
    if not s3_client:
        LOGGER.error("S3 client not initialised; cannot download %s", job.s3_key)
        return None

    key = job.resolved_key()
    try:
        LOGGER.info("Downloading cleaned HTML from s3://%s/%s", bucket, key)
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
        return body.decode("utf-8", errors="replace")
    except (ClientError, BotoCoreError) as exc:
        LOGGER.exception("Failed to download %s from bucket %s", key, bucket)
        return None


def truncate_html(html: str) -> str:
    if len(html) <= LLM_MAX_HTML_CHARS:
        return html
    LOGGER.debug("HTML truncated from %s to %s characters", len(html), LLM_MAX_HTML_CHARS)
    return html[:LLM_MAX_HTML_CHARS]


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


def estimate_token_usage(system_prompt: str, user_prompt: str) -> int:
    char_count = len(system_prompt) + len(user_prompt)
    token_estimate = max(1, char_count // TOKEN_CHAR_RATIO)
    buffer = max(1, token_estimate // 5)
    return token_estimate + buffer


def extract_total_tokens_used(response: Any) -> Optional[int]:
    usage = getattr(response, "usage", None)
    total_tokens: Optional[int] = None

    if usage is not None:
        if isinstance(usage, dict):
            total_tokens = usage.get("total_tokens")
            if total_tokens is None:
                input_tokens = usage.get("input_tokens")
                output_tokens = usage.get("output_tokens")
                if input_tokens is not None or output_tokens is not None:
                    total_tokens = (input_tokens or 0) + (output_tokens or 0)
        else:
            total_tokens = getattr(usage, "total_tokens", None)
            if total_tokens is None:
                input_tokens = getattr(usage, "input_tokens", None)
                output_tokens = getattr(usage, "output_tokens", None)
                if input_tokens is not None or output_tokens is not None:
                    total_tokens = (input_tokens or 0) + (output_tokens or 0)

    if total_tokens is None and isinstance(response, dict):
        usage_dict = response.get("usage")
        if isinstance(usage_dict, dict):
            total_tokens = usage_dict.get("total_tokens")
            if total_tokens is None:
                input_tokens = usage_dict.get("prompt_tokens") or usage_dict.get("input_tokens")
                output_tokens = usage_dict.get("completion_tokens") or usage_dict.get("output_tokens")
                if input_tokens is not None or output_tokens is not None:
                    total_tokens = (input_tokens or 0) + (output_tokens or 0)

    if total_tokens is None:
        return None
    try:
        return int(total_tokens)
    except (TypeError, ValueError):
        return None


def call_llm(job: LLMJob, html: str) -> Optional[Any]:
    metadata_json = json.dumps(job.metadata or {}, ensure_ascii=False)
    prompt_html = truncate_html(html)
    user_prompt = LLM_USER_PROMPT_TEMPLATE.format(
        source_key=job.resolved_key(),
        metadata_json=metadata_json,
        html=prompt_html,
    )
    token_estimate = estimate_token_usage(LLM_SYSTEM_PROMPT, user_prompt)

    for attempt in range(1, LLM_MAX_ATTEMPTS + 1):
        try:
            if not OPENAI_API_KEY:
                LOGGER.error("OPENAI_API_KEY not set; cannot call OpenAI API.")
                return None
            rate_limiter.wait_for_slot(token_estimate)
            LOGGER.info(
                "Calling OpenAI (attempt %s/%s) for %s",
                attempt,
                LLM_MAX_ATTEMPTS,
                job.resolved_key(),
            )
            if openai_client is not None:
                response = openai_client.responses.create(
                    model=LLM_MODEL,
                    input=[
                        {
                            "role": "system",
                            "content": [{"type": "text", "text": LLM_SYSTEM_PROMPT}],
                        },
                        {
                            "role": "user",
                            "content": [{"type": "text", "text": user_prompt}],
                        },
                    ],
                    temperature=LLM_TEMPERATURE,
                    response_format={"type": "json_object"},
                )
                content = getattr(response, "output_text", "")
                if not content and getattr(response, "output", None):
                    chunks: List[str] = []
                    for item in response.output:  # type: ignore[attr-defined]
                        for segment in item.get("content", []):
                            if segment.get("type") == "output_text":
                                chunks.append(segment.get("text", ""))
                    content = "".join(chunks)
            else:
                response = openai.ChatCompletion.create(
                    model=LLM_MODEL,
                    messages=[
                        {"role": "system", "content": LLM_SYSTEM_PROMPT},
                        {"role": "user", "content": user_prompt},
                    ],
                    temperature=LLM_TEMPERATURE,
                )
                content = response["choices"][0]["message"]["content"]
            LOGGER.debug("Raw LLM response: %s", content)
            rate_limiter.record_tokens(extract_total_tokens_used(response) or token_estimate)
            return extract_json_from_text(content)
        except Exception:
            LOGGER.exception("LLM call failed on attempt %s", attempt)
        if attempt < LLM_MAX_ATTEMPTS:
            time.sleep(LLM_RETRY_DELAY_SECONDS)
    return None


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


def ensure_iterable(value: Union[str, List[str], None]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [str(value)]


def prepare_member_record(member: Dict[str, Any], job: Optional[LLMJob]) -> Optional[Dict[str, Any]]:
    full_name = member.get("name")
    municipality = member.get("municipality")
    if job:
        municipality = municipality or job.metadata.get("municipality")
    if not full_name or not municipality:
        LOGGER.error(
            "Skipping member because 'name' or 'municipality' is missing: %s", member
        )
        return None

    record = dict(member)
    record.setdefault("municipality", municipality)
    record.setdefault("source_file", job.resolved_key() if job else None)
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


def process_payload(payload: Any, job: Optional[LLMJob]) -> None:
    count = 0
    for member in iter_member_payloads(payload):
        record = prepare_member_record(member, job)
        if not record:
            continue
        try:
            upsert_council_member(record)
            count += 1
        except Exception:
            LOGGER.exception("Failed to upsert member: %s", record)
    if count == 0:
        LOGGER.warning("No council members persisted for payload from %s", job.resolved_key() if job else "direct message")
    else:
        LOGGER.info("Persisted %s council member(s) for %s", count, job.resolved_key() if job else "direct message")


def process_job(message: str) -> None:
    try:
        parsed = parse_queue_message(message)
    except ValueError:
        LOGGER.exception("Discarding message because it could not be parsed")
        return

    if isinstance(parsed, LLMJob):
        html = download_cleaned_html(parsed)
        if html is None:
            return
        payload = call_llm(parsed, html)
        if payload is None:
            LOGGER.error("LLM did not return usable JSON for %s", parsed.resolved_key())
            return
        process_payload(payload, parsed)
    else:
        process_payload(parsed, None)


def fetch_job_batch() -> List[str]:
    batch: List[str] = []
    first_fetch = True
    while len(batch) < REDIS_BATCH_SIZE:
        if first_fetch:
            queue_name, message_bytes = redis_client.brpop(REDIS_QUEUE_NAME)
            first_fetch = False
        else:
            message_bytes = redis_client.lpop(REDIS_QUEUE_NAME)
            if message_bytes is None:
                break
        message = message_bytes.decode("utf-8")
        batch.append(message)
    LOGGER.info("Fetched %s job(s) from Redis queue.", len(batch))
    return batch


def main_worker_loop() -> None:
    LOGGER.info("Worker started. Listening on Redis queue '%s'...", REDIS_QUEUE_NAME)
    while True:
        try:
            for message in fetch_job_batch():
                process_job(message)
        except redis.ConnectionError:
            LOGGER.exception("Lost connection to Redis. Retrying in %s seconds.", REDIS_RETRY_DELAY_SECONDS)
            time.sleep(REDIS_RETRY_DELAY_SECONDS)
        except Exception:
            LOGGER.exception("Unexpected error while processing job")
            time.sleep(REDIS_RETRY_DELAY_SECONDS)


if __name__ == "__main__":
    main_worker_loop()
