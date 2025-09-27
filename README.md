rust_crawler — Distributed Web Crawler Pipeline

**Overview**
- Distributed crawler that pulls URL tasks from Redis, fetches pages through optional proxies, respects robots.txt + crawl delays, and stores results in S3.
- Julia worker cleans HTML and emits JSONL batch inputs to S3 for the OpenAI Batch API. Each JSONL line includes a stable `custom_id` derived from the source HTML filename and request metadata.
- Python DB writer discovers JSONL inputs, submits OpenAI batch jobs, polls for completion, downloads output files, and upserts one database row per JSONL output line into Postgres.
- Seeder populates initial URLs into Redis; Vector centralizes logs into Loki/Grafana.

**Architecture**
- Redis queues orchestrate work:
  - `crawler:url_queue`: pending crawl tasks (JSON `{"url","depth"}`)
  - `crawler:visited_set`: deduplicates URLs
  - `html_processing_queue`: S3 object keys of saved HTML for the Julia worker
  - `json_to_db_queue` (optional): direct JSON input to DB writer (bypass batch)
- Rust worker pulls from `crawler:url_queue`, fetches HTML, extracts links, pushes new tasks, and writes crawl metadata and selected HTML to S3.
- Julia worker downloads raw HTML from S3, removes scripts, and builds JSONL batch inputs targeting the OpenAI API; uploads JSONL files to S3 under a configured prefix.
- Python DB writer discovers JSONL in S3, creates OpenAI Batch jobs, polls job status, downloads batch output files, and upserts to Postgres.
- Log forwarding sidecar (`vector` service) tails container logs and forwards structured JSON to Redis and Loki for querying in Grafana.

**Components**
- Rust worker: `src/main.rs`
  - Config file: `worker_config.toml` (TOML). Overridable with `WORKER_CONFIG_PATH`.
  - Logs: JSON via `tracing_subscriber` (stdout).
- Seeder: `src/bin/seeder.rs`
  - Seeds `visited_set` and pushes depth-0 tasks into `crawler:url_queue`.
- Python DB writer: `python_container/db_writer.py`
  - Discovers JSONL inputs in S3, submits to the OpenAI Batch API, tracks job IDs in Redis, polls until completion, downloads output files, and upserts into Postgres (`council_members`).
  - Also supports direct messages on `json_to_db_queue` for manual/legacy ingestion (bypasses batch).
- Julia cleaner: `julia_container/clean_html_docker.jl`
  - Consumes `html_processing_queue` with S3 keys, downloads HTML, removes `<script>` tags, and generates JSONL request batches to S3 under `openai_batches/` (configurable).
  - Attaches metadata (`source_bucket`, `source_key`, `cleaned_bucket`, `jsonl_bucket`) and sets `custom_id` based on the original filename.
  - Utilizes Julia multi-threading; control threads via `JULIA_NUM_THREADS` (default: `auto`).
- Docker/Compose: `Dockerfile`, `docker-compose.yml`
  - Worker image bundles Tailscale (optional), Vector installer, and runs the Rust binary.
- Logging sidecar: `vector` service using `src/vector.toml`, forwards JSON logs to Redis.

**Prerequisites**
- Docker and Docker Compose
- An S3 bucket and AWS credentials with PutObject permissions for prefixes used by the worker.
- A Redis server reachable by all services (use `compose-docker.yml` on a central host or a managed Redis service).
- For Tailscale logging/routing (optional): a valid `TS_AUTHKEY`.

**Configuration**
- Worker config: `worker_config.toml`
  - `redis_url`: e.g., `redis://redis:6379/` (inside Compose) or `redis://127.0.0.1/` (local)
  - `s3_bucket`: S3 bucket to store crawl outputs
  - `proxy_file`: path to proxy list file (see below)
  - `max_depth`: stop queuing beyond this depth
  - `politeness_delay_ms`: base delay per host (robots.txt delay may extend this)
  - `concurrent_tasks`: number of worker tasks to spawn
  - `keywords`: if a URL contains any, also upload the raw HTML to S3
- Environment files
  - Root `.env` controls the Rust worker stack (see root `docker-compose.yml`).
  - `julia_container/.env` controls the Julia cleaner (S3/Redis and OpenAI request template settings: `S3_SOURCE_BUCKET`, `S3_DESTINATION_BUCKET`, `S3_JSONL_BUCKET`, `AWS_*`, `REDIS_*`, `OPENAI_MODEL`, `OPENAI_API_ENDPOINT`).
  - `python_container/.env` controls the DB writer (DB, Redis, S3 JSONL settings and OpenAI batch controls: `S3_JSONL_BUCKET`, `S3_JSONL_PREFIX`, `OPENAI_API_KEY`, `OPENAI_BASE_URL`, `OPENAI_BATCH_*`).
  - Optional S3-compatible settings: `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`.
  - Log forwarder target: `REDIS_LOG_ENDPOINT` (e.g. `redis://<host_or_ip>:6379/`), `REDIS_LOG_KEY` (e.g. `crawler:logs`).
  - Testing: `SAVE_ALL_HTML=true` to persist HTML for every crawled page in the Rust worker.
- Proxies file: `proxies.txt`
  - Format per line: `host:port:user:pass`
  - If file is empty/invalid, worker falls back to a direct client.
- Vector config: `src/vector.toml` (used by the `vector` service, Redis sink configurable via env).

**Build and Run (Docker Compose)**
- From the repository root:
  - `docker compose up -d redis`
  - Provide AWS creds in `.env` and set `S3_BUCKET` to an existing bucket.
  - If you have a valid Tailscale auth key, set `TS_AUTHKEY` in `.env`.
  - Build and run the worker (Vector starts automatically via `depends_on`):
    - `docker compose up --build worker`
  - Without Tailscale (local testing), bypass the entrypoint:
    - `docker compose run --rm --no-deps --entrypoint rust_crawler worker`

**Seeding URLs**
- Prepare a CSV of seed URLs or a CSV that includes a URL column.
- Example CSV provided: `Erste_10_Zeilen__Vorschau_.csv` (URLs in column `Websites`).
- Seed Redis from your host (set `REDIS_URL`, `URL_QUEUE_KEY`, `VISITED_SET_KEY` to override defaults):
  - `cargo run --bin seeder Erste_10_Zeilen__Vorschau_.csv --column-name Websites`
  - Or by index: `cargo run --bin seeder seeds.csv --column-index 0`

**What the Worker Writes**
- Crawl metadata (JSON) to S3 under `crawl-data/<uuid>.json` with fields:
  - `source_url`, `depth`, `scraped_at`, `found_links`
- Raw HTML to S3 under `crawl-html/<sanitized>.html` for URLs matching configured keywords (case-insensitive). Set `SAVE_ALL_HTML=true` to save HTML for all pages.
- The Julia cleaner converts S3 HTML keys into JSONL requests and uploads them under `openai_batches/` in `S3_JSONL_BUCKET`.

**Running the Python DB Writer (Batch mode)**
- Navigate to `python_container/` and configure `.env`:
  - Database/Redis: `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `DATABASE_URL` (optional override), `REDIS_HOST`, `REDIS_PORT`, `REDIS_QUEUE_NAME`
  - S3 JSONL inputs: `S3_JSONL_BUCKET`, `S3_JSONL_PREFIX` (default `openai_batches/`), plus `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_ENDPOINT_URL` (for MinIO)
  - OpenAI: `OPENAI_API_KEY`, `OPENAI_BASE_URL` (optional for compatible gateways)
  - Batch control: `OPENAI_BATCH_COMPLETION_WINDOW`, `OPENAI_BATCH_POLL_INTERVAL`, `OPENAI_BATCH_SUBMIT_INTERVAL`, `OPENAI_BATCH_AUTO_DISCOVER`, `OPENAI_BATCH_INPUT_LIMIT`
- Start services:
  - `docker compose --env-file .env up -d`
- Ensure a `council_members` table exists (example schema):
  - Columns: `unique_key` (PK, text), `full_name` (text), `party` (text), `municipality` (text), `roles` (jsonb), `source_file` (text), `raw_json` (jsonb), `updated_at` (timestamptz default now())
- Operation:
  - The worker auto-discovers new JSONL files in `s3://$S3_JSONL_BUCKET/$S3_JSONL_PREFIX`, submits OpenAI batch jobs, polls for completion, downloads output files, and upserts one row per JSONL output line.
  - It also accepts direct messages on `json_to_db_queue` for legacy/manual ingestion.
  - Each row contains `municipality` and `source_file` so you can correlate entries to specific Kommunen and source pages.
  - If the LLM omits a municipality, the worker can fall back to request metadata (see “Julia Cleaner and JSONL” below).

**Julia Cleaner and JSONL**
- Files: `julia_container/`
- The cleaner consumes `html_processing_queue` (S3 keys), downloads HTML, removes `<script>` tags, and constructs OpenAI batch JSONL lines:
  - `custom_id` is derived from the HTML filename for stable traceability.
  - `metadata` includes `source_bucket`, `source_key`, `cleaned_bucket`, and `jsonl_bucket`. You can optionally add a municipality hint if derivable from path/domain.
- Uploads the batch JSONL to `s3://$S3_JSONL_BUCKET/openai_batches/batch_<timestamp>_<rand>.jsonl` by default.
- Configure via `julia_container/.env`:
  - `S3_SOURCE_BUCKET`, `S3_DESTINATION_BUCKET`, `S3_JSONL_BUCKET`, `AWS_REGION`, `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`
  - `REDIS_HOST`, `REDIS_PORT`, `REDIS_QUEUE_NAME`
  - `OPENAI_MODEL`, `OPENAI_API_ENDPOINT`

**Local Development (without Docker)**
- Requirements: Rust toolchain, Redis running locally, AWS credentials in env.
- Run worker locally:
  - `WORKER_CONFIG_PATH=worker_config.toml cargo run --release`
- Update `worker_config.toml.redis_url` to `redis://127.0.0.1/` for local runs.

**Queues and Keys**
- `crawler:url_queue` — main URL queue (override via `url_queue_key` in `worker_config.toml` or `URL_QUEUE_KEY` env)
- `crawler:visited_set` — dedup set for seen URLs (`visited_set_key` / `VISITED_SET_KEY`)
- `html_processing_queue` — S3 HTML keys for Julia (`HTML_PROCESSING_QUEUE`)
- `json_to_db_queue` — optional direct input to DB writer (bypasses batch)
- S3 JSONL prefix — `s3://$S3_JSONL_BUCKET/$S3_JSONL_PREFIX` (default `openai_batches/`)

**Troubleshooting**
- Redis connection errors inside the worker container:
  - Use `redis://redis:6379/` in `worker_config.toml` (service name from Compose).
- Log forwarding issues:
  - Ensure `docker-compose.yml` volume points to `src/vector.toml`, port `24224` is not blocked, and `REDIS_LOG_ENDPOINT`/`REDIS_LOG_KEY` are set.
- Tailscale during local tests:
  - If you don’t have a valid `TS_AUTHKEY`, run the worker by overriding the entrypoint as shown above, or set a valid key.
- Proxies not working:
  - Check `proxies.txt` format; on failure the worker logs fallback to the base client.

**Security Notes**
- Do not commit real secrets. Rotate any leaked keys.
- Treat `proxies.txt` credentials as sensitive.

**Quick Start (Summary)**
- Ensure a Redis instance is running and reachable (e.g., `docker compose -f compose-docker.yml up -d` on a central host)
- Set AWS creds in `.env` and `S3_BUCKET` to an existing bucket
- Start Vector agent on the VM: `docker compose -f docker-compose.vector.yml up -d`
- Start the worker: `docker compose up --build -d worker`
- Seed URLs: `cargo run --bin seeder Erste_10_Zeilen__Vorschau_.csv --column-name Websites`
- Verify S3 objects in `crawl-data/` and `crawl-html/` as keywords match

## Centralized Logging (Loki + Grafana + Vector)

Goal: Run the logging stack (Loki + Grafana + Vector) on a central logging host, ship logs from worker VMs to that host, and access Grafana from any admin machine (via browser or SSH tunnel).

Key facts
- Run a Vector agent on every machine that runs containers you care about (central host and each worker VM). It reads Docker logs locally via the Docker socket and forwards to the central Loki.
- Redis remains an external service on the central host (or another managed instance).
- Containers are labeled so Vector knows which ones to collect: `com.rustcrawler.logs=true` and `com.rustcrawler_service=<name>`.

1) Prepare the central logging host
- Prerequisites: Docker + Docker Compose installed; Redis is already running and reachable at its LAN IP.
- In the repository root `.env`, ensure:
  - `REDIS_LOG_ENDPOINT=redis://<pi_lan_ip>:6379/` (or `localhost` if Redis is inside the same Docker host and published on 6379)
  - `LOKI_ENDPOINT=http://loki:3100` (default already set)
  - `GRAFANA_ADMIN_PASSWORD=admin` (change this in production)
- Start the logging services on the central host using the provided stack:
  - `docker compose -f compose-docker.yml up -d`
- Validate:
  - `docker compose logs -f vector` should show successful connections to Loki at `http://loki:3100`.
  - Grafana listens on `:3000` on the central host, Loki on `:3100`.

2) Start workloads on each worker VM (no local Loki/Grafana needed)
- On each VM, set the VM’s `.env`:
  - `REDIS_URL`, `REDIS_HOST` → central host IP/hostname (so workers use central Redis)
  - `LOKI_ENDPOINT=http://<central_host_ip>:3100` (so Vector forwards logs to the central Loki)
- Start each required service individually:
  - Vector agent on the VM: `docker compose -f docker-compose.vector.yml up -d`
  - Crawler worker: `docker compose up -d worker`
  - Python DB writer (standalone sub-stack): `cd python_container && docker compose --env-file .env up -d db-writer`

3) View Grafana from an admin machine via SSH tunnel
- From an admin machine, create tunnels to the central host:
  - `ssh -L 3000:localhost:3000 -L 3100:localhost:3100 <user>@<central_host>`
- Open Grafana at `http://localhost:3000` (default admin/admin unless changed by `GRAFANA_ADMIN_PASSWORD`).
- Add the Loki data source (one time in Grafana UI):
  - Data sources → Add data source → Loki → URL: `http://loki:3100` → Save & Test.
- Explore logs:
  - Use LogQL query `{project="rust_crawler"}` and filter by `service` (e.g., `|= "service=\"worker\""` or use label selector `{project="rust_crawler",service="worker"}`).

Troubleshooting
- No logs in Grafana:
  - On each VM: `docker compose logs -f vector` to confirm it can reach `http://<central_host_ip>:3100`.
  - Ensure the central host’s `3100/tcp` is reachable from worker VMs (or they are on the same LAN/VPN).
  - Confirm containers are labeled `com.rustcrawler.logs=true`.
- JSON parsing looks wrong:
  - Rust emits JSON by default; Python DB writer now uses `python-json-logger` to emit JSON; make sure the container was rebuilt after pulling changes.
- Permissions/firewall:
  - If you prefer not to expose ports, use an SSH tunnel from your admin machine. Workers still need direct network access to the central host’s Loki (3100) and Redis (6379).

Stopping
- On the central host (logging stack): `docker compose -f compose-docker.yml down`
- On a worker VM: `docker compose stop worker` and `docker compose -f docker-compose.vector.yml stop vector` (and stop `db-writer` if running)

## Deploying on a VM

This section shows how to deploy the crawler stack to any Linux VM using Docker Compose. It covers both roles: a central logging host and worker VMs running the crawler.

Prerequisites on the VM
- Docker Engine and Docker Compose plugin installed.
- Firewall opened for what you actually need:
  - Worker VM → outbound to Redis (6379/tcp) and Loki (3100/tcp) on the central host.
  - Central host (optional external access) → Grafana 3000/tcp and Loki 3100/tcp if not using SSH tunneling.
- Git or a way to copy this repo to the VM.

1) Get the code onto the VM
- SSH to the VM and clone the repo:
  - `git clone https://your/git/remote.git rust_crawler && cd rust_crawler`
- Or copy a tarball/rsync from your dev machine to the VM.

2) Configure environment
- Copy `.env` and adjust values for the VM role:
  - Common: `AWS_*`, `S3_BUCKET` and optional `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`.
  - Redis endpoint (central host):
    - `REDIS_URL=redis://<central_host_ip>:6379/`
    - `REDIS_HOST=<central_host_ip>`
  - Logging:
    - On the central logging host: keep `LOKI_ENDPOINT=http://loki:3100` (default) and set `GRAFANA_ADMIN_PASSWORD`.
    - On worker VMs: set `LOKI_ENDPOINT=http://<central_host_ip>:3100` so the local Vector forwards logs to the central host.

3A) Deploy the central logging host
- Start only the logging stack and the collector:
  - `docker compose -f compose-docker.yml up -d`
- Validate:
  - `docker compose -f compose-docker.yml ps` shows `rc-loki`, `rc-grafana`, `rc-vector`.
  - `docker compose logs -f vector` shows it connected to `http://loki:3100`.
- Access Grafana:
  - Locally on that host or via SSH tunnel from an admin machine: `ssh -L 3000:localhost:3000 -L 3100:localhost:3100 <user>@<central_host>` → open `http://localhost:3000`.

3B) Deploy a worker VM (crawler services)
- Ensure `.env` on the worker VM points to the central host for Redis and Loki (see step 2).
- Start services you need:
  - Vector agent: `docker compose -f docker-compose.vector.yml up -d`
  - Worker: `docker compose up -d worker`
  - Python DB writer (if running on this VM): `cd python_container && docker compose --env-file .env up -d db-writer`
- Check health:
  - `docker compose -f docker-compose.vector.yml logs -f vector` (should push logs to `http://<central_host_ip>:3100`).
  - `docker compose logs -f worker` for crawl activity.

4) Upgrade and restart
- Pull latest code: `git pull`.
- Rebuild images and apply: `docker compose build --pull && docker compose up -d`
- View what changed: `docker compose ps && docker image ls | head`.

5) Start on boot (optional)
- Easiest: add `restart: unless-stopped` to services in `docker-compose.yml` and reboot once to verify.
- Or create a systemd unit that runs `docker compose up -d` in this directory after Docker network is up.

Troubleshooting (VM)
- Vector can’t reach Loki on the central host:
  - Verify `LOKI_ENDPOINT=http://<central_host_ip>:3100` on the worker VM and that port 3100 is reachable.
- Workers can’t reach Redis:
  - Check `REDIS_URL`/`REDIS_HOST`, firewall/NAT, and that Redis binds to the central host’s IP.
- No logs in Grafana:
  - Ensure containers are labeled; this repo already sets `com.rustcrawler.logs=true` on `worker`, `julia_worker`, `db_writer`, and `vector`.

## Central infra: one-command start for Redis + Loki + Vector

If you prefer a minimal stack file on the central host that starts Redis, Loki, and Vector together, use `compose-docker.yml` at the repository root.

Commands
- Start: `docker compose -f compose-docker.yml up -d`
- Check: `docker compose -f compose-docker.yml ps && docker compose -f compose-docker.yml logs -f vector`
- Stop: `docker compose -f compose-docker.yml down`

Notes
- This stack publishes Redis on `6379` and Loki on `3100`. Vector tails local Docker logs via `/var/run/docker.sock` and forwards to both Redis and Loki using internal service names.
- Keep using the main `docker-compose.yml` on worker VMs; only the central host needs `compose-docker.yml`.

## Architecture Diagram

``` mermaid
flowchart LR
  subgraph Workers[Worker VMs]
    W1[Crawler Worker 1\n+Vector]
    W2[Crawler Worker 2\n+Vector]
    WN[Crawler Worker N\n+Vector]
  end

  subgraph JuliaVM[Julia VM]
    J[Julia Worker\n+Vector]
  end

  subgraph Central[Central Logging Host]
    R[Redis Server]
    L[Loki]
    G[Grafana]
    VP[Vector]
  end

  subgraph Storage[Storage and DB]
    S3[(S3/MinIO Bucket)]
    PG[(PostgreSQL)]
  end

  DBW[DB Writer Container]
  OA[OpenAI Batch API]

  %% Queues and crawl flow
  W1 -->|url_queue / visited_set| R
  W2 -->|url_queue / visited_set| R
  WN -->|url_queue / visited_set| R

  W1 -->|HTML + metadata| S3
  W2 -->|HTML + metadata| S3
  WN -->|HTML + metadata| S3

  R -->|html_processing_queue| J
  J -->|JSONL batches| S3

  DBW -->|discover JSONL| S3
  DBW -->|submit| OA
  OA  -->|output files| DBW
  DBW -->|upserts| PG

  %% Optional direct path
  R -->|json_to_db_queue (optional)| DBW

  %% Centralized logging
  W1 -.->|Vector → Loki| L
  W2 -.->|Vector → Loki| L
  WN -.->|Vector → Loki| L
  J  -.->|Vector → Loki| L
  VP -.->|Vector → Loki| L
  G <--> L

  %% Notes
  classDef note fill:#fdf6e3,stroke:#aaa,color:#333;
```

Legend
- Each VM runs a Vector agent that tails Docker logs and forwards them to Loki on the central host (`LOKI_ENDPOINT=http://<central_host_ip>:3100`). Ensure every VM starts its `vector` service.
- Redis lives on the central host and is shared by all workers. Workers read/write `url_queue` and `visited_set`; Julia consumes `html_processing_queue` and emits JSONL to S3; Python DB writer discovers JSONL, manages OpenAI batch jobs, and writes results to Postgres. The `json_to_db_queue` remains available for direct/legacy ingestion.
- S3/MinIO stores raw HTML, JSONL batch inputs, and crawl metadata.
- Grafana queries Loki for centralized viewing and search of logs.

## Docker Command Reference

### Rust worker (`crawler-worker`)
```sh
# Build
docker build -t crawler-worker .

# Run (uses repository-root .env for credentials/config)
docker run -d --name crawler-worker --env-file .env crawler-worker

# Stop and remove
docker stop crawler-worker && docker rm crawler-worker
```

### Julia cleaner (`crawler-julia`)
```sh
# Build
docker build -t julia-cleaner -f julia_container/dockerfile julia_container

# Run (uses julia_container/.env configured earlier)
docker run -d --name julia-cleaner --env-file julia_container/.env -e JULIA_NUM_THREADS=auto crawler-julia

# Stop and remove
docker stop julia-cleaner && docker rm julia-cleaner
```

### Python DB writer (`crawler-db-writer`)
```sh
# Build
docker build -t crawler-db-writer -f python_container/dockerfile python_container

# Run (requires python_container/.env with DB/Redis and Batch settings)
docker run -d --name crawler-db-writer --env-file python_container/.env crawler-db-writer

# Required in python_container/.env for batch mode:
# - OPENAI_API_KEY
# - S3_JSONL_BUCKET (and optionally S3_JSONL_PREFIX)

# Stop and remove
docker stop crawler-db-writer && docker rm crawler-db-writer
```

### Redis support container (`crawler-redis`)
```sh
# Run official Redis image (no build step needed)
docker run -d --name crawler-redis -p 6379:6379 redis:6

# Stop and remove
docker stop crawler-redis && docker rm crawler-redis
```

## End-to-End Flow (Checklist)

1) Configure environment files
- Root `.env` for the Rust worker stack (S3 creds/bucket, Redis, optional `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`).
- `julia_container/.env`:
  - `AWS_*`, `S3_SOURCE_BUCKET`, `S3_DESTINATION_BUCKET`, `S3_JSONL_BUCKET`
  - `REDIS_HOST`, `REDIS_PORT`, `REDIS_QUEUE_NAME=html_processing_queue`
  - `OPENAI_MODEL`, `OPENAI_API_ENDPOINT` (used only for JSONL request template)
- `python_container/.env`:
  - DB/Redis: `POSTGRES_*` or `DATABASE_URL`, `REDIS_HOST`, `REDIS_PORT`, `REDIS_QUEUE_NAME`
  - S3 JSONL input: `S3_JSONL_BUCKET`, `S3_JSONL_PREFIX` (default `openai_batches/`), `AWS_*`
  - OpenAI Batch: `OPENAI_API_KEY`, optionally `OPENAI_BASE_URL`, and cadence vars `OPENAI_BATCH_*`

2) Start core services
- Central logging stack (optional, from repo root): `docker compose -f compose-docker.yml up -d`
- Redis (if not using the central stack): `docker run -d --name crawler-redis -p 6379:6379 redis:6`

3) Start the Rust crawler
- From repo root: `docker compose up -d worker`
- Verify it enqueues S3 HTML keys to `html_processing_queue` and writes HTML to S3 (watch `docker compose logs -f worker`).

4) Start the Julia cleaner
- From `julia_container/`: `docker compose --env-file .env up -d`
- Verify logs show JSONL batches uploaded to `s3://$S3_JSONL_BUCKET/$S3_JSONL_PREFIX` (default `openai_batches/`).

5) Start the Python DB writer (batch manager)
- From `python_container/`: `docker compose --env-file .env up -d db-writer`
- Confirm logs show batch submission (OpenAI job IDs recorded), polling, and completion, followed by output file ingestion.

6) Verify data in Postgres
- Example queries:
```sql
SELECT municipality, count(*) FROM council_members GROUP BY municipality ORDER BY count DESC;
SELECT full_name, party, roles, source_file FROM council_members WHERE municipality = 'Musterstadt' ORDER BY full_name;
```

7) Quick pipeline smoke test options
- Push a direct JSON test to bypass batch (legacy path):
  `redis-cli -h <redis_host> -p 6379 LPUSH json_to_db_queue '{"name":"Max Muster","gemeinde":"Musterstadt","partei":"ABC","rollen":["Rat"],"quelle":"datei.html"}'`
- Or push one S3 HTML key to kick Julia → JSONL → Batch path:
  `redis-cli -h <redis_host> -p 6379 LPUSH html_processing_queue 'crawl-html/example.html'` (ensure the object exists in `S3_SOURCE_BUCKET`).

8) Observability
- Vector logs to Loki: `docker compose -f docker-compose.vector.yml logs -f vector` (on each VM) and explore in Grafana.
- Service logs: `docker compose logs -f worker`, `docker compose -f julia_container/docker-compose.yml logs -f`, `docker compose -f python_container/docker-compose.yml logs -f db-writer`.

9) Common pitfalls
- Buckets/prefixes must exist; `S3_JSONL_BUCKET` must be readable by the DB writer.
- When using MinIO, set `AWS_ENDPOINT_URL` and allow HTTP with `AWS_ALLOW_HTTP=true` where applicable.
- Ensure `OPENAI_API_KEY` is set for the DB writer; without it, batch submission will fail.
