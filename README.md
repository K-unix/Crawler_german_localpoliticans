rust_crawler — Distributed Web Crawler Pipeline

**Overview**
- Distributed crawler that pulls URL tasks from Redis, fetches pages through optional proxies, respects robots.txt + crawl delays, and stores results in S3.
- Includes a seeder to populate initial URLs, a Python DB writer to persist LLM-extracted council data into Postgres, and an optional Julia worker to clean HTML.

**Architecture**
- Redis queues orchestrate work:
  - `crawler:url_queue`: pending crawl tasks (JSON `{"url","depth"}`)
  - `crawler:visited_set`: deduplicates URLs
  - `json_to_db_queue`: Python DB writer input (JSON with council member data)
- Rust worker pulls from `crawler:url_queue`, fetches HTML, extracts links, pushes new tasks, and writes crawl metadata/HTML to S3.
- Log forwarding sidecar (`vector` service) tails container logs and forwards structured JSON to:
  - Redis (list key `REDIS_LOG_KEY`) and
  - Loki (log store) for querying in Grafana.

**Components**
- Rust worker: `src/main.rs`
  - Config file: `worker_config.toml` (TOML). Overridable with `WORKER_CONFIG_PATH`.
  - Logs: JSON via `tracing_subscriber` (stdout).
- Seeder: `src/bin/seeder.rs`
  - Seeds `visited_set` and pushes depth-0 tasks into `crawler:url_queue`.
- Python DB writer: `python_container/db_writer.py`
  - Listens on `json_to_db_queue`, upserts into Postgres table `council_members`.
- Julia cleaner (optional): `julia_container/clean_html_docker.jl`
  - Consumes a Redis queue and round-trips HTML from S3, cleaning it.
  - Utilizes Julia multi-threading to process batch items in parallel. Control threads via `JULIA_NUM_THREADS` (default: `auto`).
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
- Env file for Compose: `.env`
  - `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `S3_BUCKET`
  - `TS_AUTHKEY` (required if using Tailscale in the worker container)
  - Optional S3-compatible settings: `AWS_ENDPOINT_URL`, `AWS_ALLOW_HTTP`
  - Redis endpoints/keys: `REDIS_URL`, `REDIS_HOST`, `URL_QUEUE_KEY`, `VISITED_SET_KEY`
  - Queue wiring: `HTML_PROCESSING_QUEUE`, `CLEANED_HTML_NEXT_QUEUE`, `DB_WRITER_QUEUE`
  - Consumer tuning: `REDIS_BATCH_SIZE` (default 5)
  - Log forwarder target: `REDIS_LOG_ENDPOINT` (e.g. `redis://<host_or_ip>:6379/`), `REDIS_LOG_KEY` (e.g. `crawler:logs`)
  - Testing: `SAVE_ALL_HTML=true` to persist HTML for every crawled page
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
- Raw HTML to S3 under `crawl-html/<sanitized>.html` for URLs matching configured keywords (case-insensitive). Set `SAVE_ALL_HTML=true` to save HTML for all pages (useful for testing MinIO/S3 setup).

**Running the Python DB Writer**
- Navigate to `python_container/`.
- Configure `.env` with:
  - `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
  - `REDIS_QUEUE_NAME` (default: `json_to_db_queue`)
  - `REDIS_BATCH_SIZE` (default: 5)
- Start services:
  - `docker compose --env-file .env up -d`
- Ensure a `council_members` table exists (example schema):
  - Columns: `unique_key` (PK, text), `full_name` (text), `party` (text), `municipality` (text),
    `roles` (jsonb), `source_file` (text), `raw_json` (jsonb), `updated_at` (timestamptz default now())
- Push a test message to the queue to verify upsert path:
  - `redis-cli -h 127.0.0.1 -p 6379 LPUSH json_to_db_queue '{"name":"Max Muster","municipality":"Musterstadt","party":"ABC","roles":["Rat"],"source_file":"file.html"}'`

**Optional: Julia Cleaner**
- Files: `julia_container/`
- Ensure the Dockerfile `CMD` matches the actual script (provided script: `clean_html_docker.jl`).
- Provide AWS/Redis envs as in `julia_container/docker-compose.yml`.

**Local Development (without Docker)**
- Requirements: Rust toolchain, Redis running locally, AWS credentials in env.
- Run worker locally:
  - `WORKER_CONFIG_PATH=worker_config.toml cargo run --release`
- Update `worker_config.toml.redis_url` to `redis://127.0.0.1/` for local runs.

**Queues and Keys**
- `crawler:url_queue` — main URL queue (override via `url_queue_key` in `worker_config.toml` or `URL_QUEUE_KEY` env)
- `crawler:visited_set` — dedup set for seen URLs (`visited_set_key` / `VISITED_SET_KEY`)
- `html_processing_queue` — cleaned HTML fan-out to Julia (`HTML_PROCESSING_QUEUE`)
- `json_to_db_queue` — Python DB writer input (`DB_WRITER_QUEUE` or `REDIS_QUEUE_NAME`)

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

  %% Queues and crawl flow
  W1 -->|url_queue / visited_set| R
  W2 -->|url_queue / visited_set| R
  WN -->|url_queue / visited_set| R

  W1 -->|HTML + metadata| S3
  W2 -->|HTML + metadata| S3
  WN -->|HTML + metadata| S3

  R -->|html_processing_queue| J
  J -->|cleaned HTML| S3

  R -->|json_to_db_queue| DBW
  DBW -->|fetch cleaned HTML| S3
  DBW -->|upserts| PG

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
- Redis lives on the central host and is shared by all workers. Workers read/write `url_queue` and `visited_set`; Julia consumes `html_processing_queue`; Python DB writer consumes `json_to_db_queue`.
- S3/MinIO stores raw and cleaned HTML plus crawl metadata. The DB writer fetches cleaned HTML from S3 and writes structured rows to Postgres.
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

# Run (requires python_container/.env with database/Redis settings)
docker run -d --name crawler-db-writer --env-file python_container/.env crawler-db-writer

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
