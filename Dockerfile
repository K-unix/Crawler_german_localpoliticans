FROM rust:latest AS builder
# Set the working directory
WORKDIR /usr/src/crawler

# Copy the entire project
COPY . .

# Build the worker and seeder binaries in release mode for performance.
# This leverages Docker's layer caching.
RUN cargo build --release


# =========================================================================
# Stage 2: The Final Image (no Tailscale)
# =========================================================================
FROM debian:latest

ENV TZ=Europe/Berlin
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Minimal runtime deps
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates dnsutils curl \
    && rm -rf /var/lib/apt/lists/*

# Trust proxy provider certificate chain
COPY certs/proxy-webshare.crt /usr/local/share/ca-certificates/proxy-webshare.crt
RUN update-ca-certificates

# Configure the crawler
RUN mkdir -p /etc/crawler

# Copy compiled binary
COPY --from=builder /usr/src/crawler/target/release/rust_crawler /usr/local/bin/

# Copy configuration files
COPY worker_config.toml /etc/crawler/
COPY proxies.txt /etc/crawler/

# Working directory so the worker can find its config file
WORKDIR /etc/crawler

# Environment
ENV RUST_LOG=info
ENV WORKER_CONFIG_PATH=/etc/crawler/worker_config.toml
ENV AWS_REGION=eu-central-1
ENV S3_BUCKET=test-data


# Run the worker directly (no Tailscale entrypoint)
CMD ["rust_crawler"]
