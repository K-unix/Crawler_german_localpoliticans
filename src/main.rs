use std::{
    error::Error,
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio::time::sleep;

use aws_config::{BehaviorVersion, meta::region::RegionProviderChain};
use aws_sdk_s3::{Client as S3Client, primitives::ByteStream};
use config::{Config as SettingsLoader, File, FileFormat};
use dashmap::DashMap;
use lol_html::{HtmlRewriter, Settings, element};
use redis::AsyncCommands;
use reqwest::{
    Client, Proxy, StatusCode,
    header::{ACCEPT, ACCEPT_LANGUAGE, CONTENT_TYPE, HeaderMap, HeaderValue},
    redirect::Policy,
};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

type DynError = Box<dyn Error + Send + Sync>;

const SCRIPT_ADD_IF_NOT_EXISTS: &str = r"
    local added_count = 0
    -- KEYS[1] is visited_set, KEYS[2] is url_queue
    for i, url_json in ipairs(ARGV) do
        -- FIX: Use the built-in, safe cjson library to parse the task
        local task = cjson.decode(url_json)
        local url_to_check = task.url

        if url_to_check and redis.call('SISMEMBER', KEYS[1], url_to_check) == 0 then
            redis.call('SADD', KEYS[1], url_to_check)
            redis.call('LPUSH', KEYS[2], url_json)
            added_count = added_count + 1
        end
    end
    return added_count
";

const USER_AGENT: &str = "Mozilla/5.0 (compatible; DistributedCrawler/1.0)";
const PER_URL_TIMEOUT: Duration = Duration::from_secs(60);

// --- CONFIG & DATA STRUCTS ---
fn default_url_queue_key() -> String {
    "crawler:url_queue".to_string()
}
fn default_visited_set_key() -> String {
    "crawler:visited_set".to_string()
}

#[derive(Debug, Deserialize)]
struct WorkerConfig {
    redis_url: String,
    s3_bucket: String,
    proxy_file: String,
    max_depth: u32,
    politeness_delay_ms: u64,
    keywords: Vec<String>,
    concurrent_tasks: usize,
    #[serde(default = "default_url_queue_key")]
    url_queue_key: String,
    #[serde(default = "default_visited_set_key")]
    visited_set_key: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct UrlTask {
    url: String,
    depth: u32,
}

#[derive(Serialize)]
struct CrawlResult {
    source_url: String,
    depth: u32,
    scraped_at: String,
    found_links: Vec<String>,
}

// ================================================================================================
// YOUR HELPER FUNCTIONS (UNCHANGED)
// ================================================================================================
#[derive(Clone, Debug, Default)]
struct RobotsPolicy {
    allow: Vec<String>,
    disallow: Vec<String>,
    crawl_delay: Option<Duration>,
}
impl RobotsPolicy {
    fn is_allowed(&self, path: &str) -> bool {
        let longest = |rules: &Vec<String>| -> usize {
            rules
                .iter()
                .filter(|p| path.starts_with(p.as_str()))
                .map(|p| p.len())
                .max()
                .unwrap_or(0)
        };
        let (a, d) = (longest(&self.allow), longest(&self.disallow));
        if d > a && d > 0 { false } else { true }
    }
}
#[derive(Clone)]
struct RobotsManager {
    cache: Arc<DashMap<String, Arc<RobotsPolicy>>>,
    last_fetch: Arc<DashMap<String, Arc<Mutex<Instant>>>>,
    user_agent: String,
}
impl RobotsManager {
    fn new(user_agent: String) -> Self {
        Self {
            cache: Arc::new(DashMap::new()),
            last_fetch: Arc::new(DashMap::new()),
            user_agent,
        }
    }
    fn host_key(url: &Url) -> String {
        format!(
            "{}://{}{}",
            url.scheme(),
            url.host_str().unwrap_or(""),
            url.port().map(|p| format!(":{}", p)).unwrap_or_default()
        )
    }
    async fn get_policy(&self, client: &reqwest::Client, url: &Url) -> Arc<RobotsPolicy> {
        let key = Self::host_key(url);
        if let Some(found) = self.cache.get(&key) {
            return found.clone();
        }
        let robots_url = format!("{}/robots.txt", key);
        let text = match client.get(&robots_url).send().await {
            Ok(r) => r.text().await.unwrap_or_default(),
            Err(_) => String::new(),
        };
        let policy = Arc::new(parse_robots(&text, &self.user_agent));
        self.cache.insert(key.clone(), policy.clone());
        self.last_fetch
            .entry(key)
            .or_insert_with(|| Arc::new(Mutex::new(Instant::now() - Duration::from_secs(3600))));
        policy
    }
    async fn apply_crawl_delay(&self, url: &Url, delay: Option<Duration>) {
        if let Some(delay) = delay {
            let key = Self::host_key(url);
            if let Some(lock) = self.last_fetch.get(&key).map(|entry| entry.value().clone()) {
                let mut last = lock.lock().await;
                let elapsed = last.elapsed();
                if elapsed < delay {
                    sleep(delay - elapsed).await;
                }
                *last = Instant::now();
            }
        }
    }
}

enum DeadlineAwaitError<E> {
    Timeout,
    Error(E),
}

async fn await_result_with_deadline<F, T, E>(
    deadline: Instant,
    fut: F,
) -> Result<T, DeadlineAwaitError<E>>
where
    F: Future<Output = Result<T, E>>,
{
    let now = Instant::now();
    let remaining = match deadline.checked_duration_since(now) {
        Some(d) if !d.is_zero() => d,
        _ => return Err(DeadlineAwaitError::Timeout),
    };
    match tokio::time::timeout(remaining, fut).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(DeadlineAwaitError::Error(err)),
        Err(_) => Err(DeadlineAwaitError::Timeout),
    }
}
fn parse_robots(text: &str, ua: &str) -> RobotsPolicy {
    /* ... your existing parse_robots function ... */
    #[derive(Default)]
    struct Group {
        uas: Vec<String>,
        allow: Vec<String>,
        disallow: Vec<String>,
        delay: Option<Duration>,
    }
    let mut groups: Vec<Group> = Vec::new();
    let mut cur = Group::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut parts = line.splitn(2, ':');
        let (k, v) = match (parts.next(), parts.next()) {
            (Some(k), Some(v)) => (k.trim().to_ascii_lowercase(), v.trim()),
            _ => continue,
        };
        match k.as_str() {
            "user-agent" => {
                if !(cur.allow.is_empty() && cur.disallow.is_empty() && cur.delay.is_none())
                    || !cur.uas.is_empty()
                {
                    groups.push(std::mem::take(&mut cur));
                }
                cur.uas.push(v.to_string());
            }
            "allow" => cur.allow.push(v.to_string()),
            "disallow" => cur.disallow.push(v.to_string()),
            "crawl-delay" => {
                if let Ok(sec) = v.parse::<f64>() {
                    cur.delay = Some(Duration::from_millis((sec * 1000.0).ceil() as u64));
                }
            }
            _ => {}
        }
    }
    if !cur.uas.is_empty()
        || !cur.allow.is_empty()
        || !cur.disallow.is_empty()
        || cur.delay.is_some()
    {
        groups.push(cur);
    }
    let ua_lc = ua.to_ascii_lowercase();
    let mut chosen: Vec<&Group> = Vec::new();
    let mut had_specific = false;
    for g in &groups {
        let mut this_is_specific = false;
        let mut matches = false;
        for gua in &g.uas {
            let gua_lc = gua.to_ascii_lowercase();
            if gua_lc == "*" {
                matches = true;
            } else if ua_lc.contains(&gua_lc) || gua_lc.contains(&ua_lc) {
                matches = true;
                this_is_specific = true;
            }
        }
        if matches {
            if this_is_specific && !had_specific {
                chosen.clear();
                had_specific = true;
            }
            if !had_specific || this_is_specific {
                chosen.push(g);
            }
        }
    }
    if chosen.is_empty() {
        return RobotsPolicy::default();
    }
    let mut policy = RobotsPolicy::default();
    for g in chosen {
        policy.allow.extend(g.allow.iter().cloned());
        policy.disallow.extend(g.disallow.iter().cloned());
        if policy.crawl_delay.is_none() {
            policy.crawl_delay = g.delay;
        }
    }
    policy
}
fn build_base_client(ua: &str) -> Result<Client, DynError> {
    let mut default_headers = HeaderMap::new();
    default_headers.insert(
        ACCEPT,
        HeaderValue::from_static("text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"),
    );
    default_headers.insert(
        ACCEPT_LANGUAGE,
        HeaderValue::from_static("de-DE,de;q=0.9,en;q=0.8"),
    );
    let client = Client::builder()
        .user_agent(ua)
        .default_headers(default_headers)
        .timeout(Duration::from_secs(20))
        .redirect(Policy::limited(10))
        //.http2_adaptive_window(true)
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(16)
        .pool_idle_timeout(Duration::from_secs(30))
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .tcp_nodelay(true)
        .build()?;
    Ok(client)
}
#[derive(Clone, Debug)]
struct ProxyMetadata {
    host: String,
    port: u16,
    username: Option<String>,
    uses_auth: bool,
}

impl ProxyMetadata {
    fn http_endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    fn username(&self) -> &str {
        self.username.as_deref().unwrap_or("")
    }
}

struct ProxyClient {
    client: Client,
    metadata: ProxyMetadata,
}

fn build_clients_from_proxy_file(path: &str, ua: &str) -> Result<Vec<ProxyClient>, DynError> {
    tracing::info!(file = path, "Loading proxy configuration");
    let mut clients = Vec::new();
    let content = std::fs::read_to_string(path)?;
    for (lineno, raw) in content.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split(':').collect();
        let line_number = lineno + 1;
        if parts.len() != 4 {
            tracing::warn!(
                file = path,
                line = line_number,
                "Skipping proxy entry due to bad format; expected host:port:user:pass"
            );
            continue;
        }
        let host = parts[0].trim();
        let port_raw = parts[1].trim();
        let username_raw = parts[2].trim();
        let password_raw = parts[3].trim();
        let port: u16 = match port_raw.parse() {
            Ok(p) => p,
            Err(_) => {
                tracing::warn!(
                    file = path,
                    line = line_number,
                    port = port_raw,
                    "Skipping proxy entry due to invalid port value"
                );
                continue;
            }
        };
        let proxy_url = format!("http://{}:{}", host, port);
        let uses_auth = !username_raw.is_empty() || !password_raw.is_empty();
        tracing::info!(
            file = path,
            line = line_number,
            proxy_endpoint = %proxy_url,
            uses_auth,
            "Configuring HTTP proxy client"
        );
        let proxy = if uses_auth {
            Proxy::all(&proxy_url)?.basic_auth(username_raw, password_raw)
        } else {
            Proxy::all(&proxy_url)?
        };
        let mut default_headers = HeaderMap::new();
        default_headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            ),
        );
        default_headers.insert(
            ACCEPT_LANGUAGE,
            HeaderValue::from_static("de-DE,de;q=0.9,en;q=0.8"),
        );
        let allow_insecure_tls = std::env::var("ALLOW_INSECURE_PROXY_TLS")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        let mut builder = Client::builder()
            .user_agent(ua)
            .default_headers(default_headers)
            .timeout(Duration::from_secs(20))
            .redirect(Policy::limited(10))
            .http1_only()
            .connect_timeout(Duration::from_secs(10))
            .pool_max_idle_per_host(16)
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .tcp_nodelay(true)
            .proxy(proxy);
        if allow_insecure_tls {
            builder = builder
                .danger_accept_invalid_certs(true)
                .danger_accept_invalid_hostnames(true);
        }
        let client = builder.build()?;
        let metadata = ProxyMetadata {
            host: host.to_string(),
            port,
            username: if username_raw.is_empty() {
                None
            } else {
                Some(username_raw.to_string())
            },
            uses_auth,
        };
        tracing::debug!(
            file = path,
            line = line_number,
            proxy_endpoint = %metadata.http_endpoint(),
            proxy_user = metadata.username(),
            "Proxy client built"
        );
        clients.push(ProxyClient { client, metadata });
    }
    if clients.is_empty() {
        return Err("no usable proxies loaded from file".into());
    }
    tracing::info!(
        file = path,
        proxy_count = clients.len(),
        "Loaded proxy clients"
    );
    Ok(clients)
}
fn normalize_url(url: &mut Url) {
    /* ... your existing normalize_url function ... */
    url.set_fragment(None);
    if (url.scheme() == "http" && url.port() == Some(80))
        || (url.scheme() == "https" && url.port() == Some(443))
    {
        url.set_port(None).unwrap();
    }
    if url.path().is_empty() {
        url.set_path("/");
    }
}
fn is_http_scheme(url: &Url) -> bool {
    let s = url.scheme();
    s == "http" || s == "https"
}

fn matches_keywords(url: &Url, keywords: &[String]) -> bool {
    if keywords.is_empty() {
        return false;
    }
    let hay = url.as_str().to_ascii_lowercase();
    keywords
        .iter()
        .filter_map(|needle| {
            let n = needle.trim();
            if n.is_empty() {
                None
            } else {
                Some(n.to_ascii_lowercase())
            }
        })
        .any(|needle_lc| hay.contains(&needle_lc))
}

// Add this function to create a safe filename from a URL
fn safe_filename_from_url(url: &Url) -> String {
    let mut s = String::new();
    if let Some(h) = url.host_str() {
        s.push_str(h);
    }
    s.push('_');
    s.push_str(url.path());
    if let Some(q) = url.query() {
        s.push('_');
        s.push_str(q);
    }

    let mut out_buf = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '.' {
            out_buf.push(ch);
        } else {
            out_buf.push('_');
        }
    }
    while out_buf.contains("__") {
        out_buf = out_buf.replace("__", "_");
    }
    let out = out_buf
        .trim_matches('_')
        .chars()
        .take(150)
        .collect::<String>();
    if out.is_empty() {
        "index".to_string()
    } else {
        out
    }
}

// ================================================================================================
// NEW/MODIFIED DISTRIBUTED WORKER LOGIC
// ================================================================================================

async fn save_data_to_s3(
    s3_client: &S3Client,
    bucket: &str,
    result: &CrawlResult,
) -> Result<(), DynError> {
    let content = serde_json::to_string_pretty(result)?;
    let key = format!("crawl-data/{}.json", Uuid::new_v4());
    let body = ByteStream::from(content.into_bytes());
    s3_client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;
    Ok(())
}

async fn save_html_to_s3(
    s3_client: &S3Client,
    bucket: &str,
    url: &Url,
    html_body: &[u8],
) -> Result<String, DynError> {
    // We'll save the HTML to a different "folder" in S3 to keep things organized.
    let filename = safe_filename_from_url(url);
    let key = format!("crawl-html/{}.html", filename);

    let body = ByteStream::from(html_body.to_vec());

    s3_client
        .put_object()
        .bucket(bucket)
        .key(key.clone())
        .content_type("text/html; charset=utf-8")
        .body(body)
        .send()
        .await?;

    Ok(key)
}

fn log_idle_queue(worker_id: usize, queue: &str) {
    tracing::info!(
        worker_id = worker_id,
        queue = %queue,
        "No jobs found in Redis queue; worker will retry"
    );
}

async fn worker_loop(worker_id: usize, settings: Arc<WorkerConfig>) -> Result<(), DynError> {
    tracing::info!(worker_id = worker_id, "Starting worker");

    // --- SETUP ---
    let user_agent: String = USER_AGENT.to_string();

    // 1. Connect to Redis
    let redis_client = redis::Client::open(settings.redis_url.clone())?;
    let mut redis_con = redis_client.get_multiplexed_async_connection().await?;
    tracing::info!(worker_id = worker_id, "Connected to Redis");

    let url_queue_key = settings.url_queue_key.clone();
    let visited_set_key = settings.visited_set_key.clone();

    // 2. Setup S3 Client
    let region_provider = RegionProviderChain::default_provider().or_else("eu-central-1");
    let aws_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .load()
        .await;
    let mut s3_cfg_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Ok(endpoint) = std::env::var("AWS_ENDPOINT_URL") {
        // Support S3-compatible endpoints (e.g., MinIO) and HTTP
        s3_cfg_builder = s3_cfg_builder.endpoint_url(endpoint).force_path_style(true);
    }
    let s3_client = S3Client::from_conf(s3_cfg_builder.build());
    let s3_bucket = settings.s3_bucket.clone();
    tracing::info!(worker_id = worker_id, bucket = %s3_bucket, "S3 client configured");

    // 3. Setup Reqwest Client Pool
    let backbone_client = build_base_client(&user_agent)?;
    let (proxy_clients, proxy_pool_active) =
        match build_clients_from_proxy_file(&settings.proxy_file, &user_agent) {
            Ok(clients) => (clients, true),
            Err(err) => {
                tracing::warn!(
                    worker_id = worker_id,
                    error = ?err,
                    "Falling back to backbone connection because proxies could not be loaded"
                );
                (Vec::new(), false)
            }
        };
    if proxy_pool_active {
        tracing::info!(
            worker_id = worker_id,
            proxy_count = proxy_clients.len(),
            "Initialized HTTP proxy pool"
        );
    } else {
        tracing::info!(
            worker_id = worker_id,
            "Using backbone connection for all requests"
        );
    }
    let rr_counter = Arc::new(AtomicUsize::new(0));
    let robots = Arc::new(RobotsManager::new(user_agent.clone()));
    let politeness_delay = Duration::from_millis(settings.politeness_delay_ms);

    // --- THE MAIN LOOP ---
    tracing::info!(worker_id = worker_id, "Waiting for URLs");
    let save_all_html = std::env::var("SAVE_ALL_HTML")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let html_queue: String = std::env::var("HTML_PROCESSING_QUEUE")
        .unwrap_or_else(|_| "html_processing_queue".to_string());
    'task: loop {
        // 1. Fetch a URL Task from the queue
        let result: Option<(String, String)> = redis_con.brpop(url_queue_key.as_str(), 5.0).await?;
        let task_json = match result {
            Some((_, json)) => json,
            None => {
                log_idle_queue(worker_id, &url_queue_key);
                continue;
            }
        };

        let task: UrlTask = match serde_json::from_str(&task_json) {
            Ok(t) => t,
            Err(e) => {
                tracing::error!(worker_id = worker_id, error = ?e, "Could not parse task JSON");
                continue;
            }
        };

        let current_url = match Url::parse(&task.url) {
            Ok(u) => u,
            Err(_) => continue,
        };

        if let Err(e) = redis_con
            .sadd::<_, _, i32>(visited_set_key.as_str(), current_url.as_str())
            .await
        {
            tracing::error!(
                worker_id = worker_id,
                error = ?e,
                url = %current_url,
                "Failed to mark URL as visited"
            );
        }

        // 2. Perform the Crawl
        let (client, proxy_meta_opt): (&Client, Option<&ProxyMetadata>) = if proxy_pool_active {
            let proxy_count = proxy_clients.len();
            let idx = rr_counter.fetch_add(1, Ordering::Relaxed) % proxy_count;
            let entry = &proxy_clients[idx];
            tracing::info!(
                worker_id = worker_id,
                proxy_index = idx,
                proxy_count,
                proxy_endpoint = %entry.metadata.http_endpoint(),
                proxy_user = entry.metadata.username(),
                proxy_uses_auth = entry.metadata.uses_auth,
                "Dispatching request via HTTP proxy"
            );
            (&entry.client, Some(&entry.metadata))
        } else {
            tracing::info!(
                worker_id = worker_id,
                "Dispatching request via backbone connection"
            );
            (&backbone_client, None)
        };

        let policy = robots.get_policy(client, &current_url).await;
        let policy_crawl_delay = policy.crawl_delay;
        let apply_url_timeout = policy_crawl_delay.unwrap_or_default() < PER_URL_TIMEOUT;
        let url_deadline = Instant::now() + PER_URL_TIMEOUT;

        if !policy.is_allowed(current_url.path()) {
            continue;
        }

        let mut effective_delay = politeness_delay;
        if let Some(d) = policy_crawl_delay {
            if d > effective_delay {
                effective_delay = d;
            }
        }
        if !effective_delay.is_zero() {
            sleep(effective_delay).await;
        }

        robots
            .apply_crawl_delay(&current_url, policy_crawl_delay)
            .await;

        tracing::info!(worker_id = worker_id, depth = task.depth, url = %current_url, "Crawling URL");
        let mut active_client = client;
        let mut active_meta = proxy_meta_opt;
        let mut attempt = 0usize;

        let resp = loop {
            let send_future = active_client.get(current_url.as_str()).send();
            let request_result = if apply_url_timeout {
                match await_result_with_deadline(url_deadline, send_future).await {
                    Ok(r) => Ok(r),
                    Err(DeadlineAwaitError::Timeout) => {
                        tracing::warn!(
                            worker_id = worker_id,
                            url = %current_url,
                            "HTTP request exceeded per-URL timeout, skipping"
                        );
                        continue 'task;
                    }
                    Err(DeadlineAwaitError::Error(err)) => Err(err),
                }
            } else {
                match send_future.await {
                    Ok(r) => Ok(r),
                    Err(err) => Err(err),
                }
            };

            match request_result {
                Ok(response) => break response,
                Err(err) => {
                    if active_meta.is_some() && err.is_connect() && attempt == 0 {
                        tracing::warn!(
                            worker_id = worker_id,
                            url = %current_url,
                            error = ?err,
                            "Proxy connection failed, retrying via backbone connection"
                        );
                        active_client = &backbone_client;
                        active_meta = None;
                        attempt += 1;
                        continue;
                    } else {
                        tracing::warn!(
                            worker_id = worker_id,
                            url = %current_url,
                            error = ?err,
                            "HTTP request failed"
                        );
                        continue 'task;
                    }
                }
            }
        };

        let status = resp.status();
        let proxied = active_meta.is_some();
        let remote_addr = resp
            .remote_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        tracing::info!(
            worker_id = worker_id,
            url = %current_url,
            http_status = %status,
            proxied,
            remote_addr = %remote_addr,
            proxy_endpoint = active_meta.map(|m| m.http_endpoint()),
            proxy_user = active_meta.map(|m| m.username().to_string()),
            "Received HTTP response"
        );

        if status == StatusCode::PROXY_AUTHENTICATION_REQUIRED {
            if let Some(meta) = active_meta {
                tracing::error!(
                    worker_id = worker_id,
                    url = %current_url,
                    proxy_endpoint = %meta.http_endpoint(),
                    proxy_user = meta.username(),
                    proxy_uses_auth = meta.uses_auth,
                    http_status = %status,
                    "Proxy authentication failed; verify credentials or available credits"
                );
            } else {
                tracing::error!(
                    worker_id = worker_id,
                    url = %current_url,
                    http_status = %status,
                    "Proxy authentication failed; received HTTP 407"
                );
            }
        }

        let ctype = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok());
        if !status.is_success() || !ctype.map_or(false, |ct| ct.starts_with("text/html")) {
            continue;
        }

        let body_future = resp.bytes();
        let body = if apply_url_timeout {
            match await_result_with_deadline(url_deadline, body_future).await {
                Ok(b) => b,
                Err(DeadlineAwaitError::Timeout) => {
                    tracing::warn!(worker_id = worker_id, url = %current_url, "Reading response body exceeded per-URL timeout, skipping");
                    continue;
                }
                Err(DeadlineAwaitError::Error(_)) => continue,
            }
        } else {
            match body_future.await {
                Ok(b) => b,
                Err(_) => continue,
            }
        };

        if apply_url_timeout && Instant::now() >= url_deadline {
            tracing::warn!(worker_id = worker_id, url = %current_url, "Per-URL timeout exceeded after download, skipping further processing");
            continue;
        }
        // --- ADD THIS BLOCK TO SAVE HTML FOR KEYWORD MATCHES ---
        if save_all_html || matches_keywords(&current_url, &settings.keywords) {
            if apply_url_timeout && Instant::now() >= url_deadline {
                tracing::warn!(worker_id = worker_id, url = %current_url, "Per-URL timeout exceeded before HTML upload, skipping");
                continue;
            }
            tracing::info!(worker_id = worker_id, url = %current_url, "Keyword match found, saving HTML");
            let html_future = save_html_to_s3(&s3_client, &s3_bucket, &current_url, &body);
            let html_result = if apply_url_timeout {
                match await_result_with_deadline(url_deadline, html_future).await {
                    Ok(res) => Ok(res),
                    Err(DeadlineAwaitError::Timeout) => {
                        tracing::warn!(worker_id = worker_id, url = %current_url, "HTML upload exceeded per-URL timeout, skipping");
                        continue;
                    }
                    Err(DeadlineAwaitError::Error(e)) => Err(e),
                }
            } else {
                html_future.await
            };
            match html_result {
                Ok(s3_key) => {
                    // Push the S3 key to the HTML processing queue for the Julia worker
                    if let Err(e) = redis_con.lpush::<_, _, ()>(&html_queue, &s3_key).await {
                        tracing::error!(worker_id = worker_id, error = ?e, key = %s3_key, "Failed to enqueue HTML key for processing");
                    } else {
                        tracing::info!(worker_id = worker_id, key = %s3_key, queue = %html_queue, "Enqueued HTML key for processing");
                    }
                }
                Err(e) => {
                    tracing::error!(worker_id = worker_id, error = ?e, "Failed to save HTML to S3")
                }
            }
        }

        // 3. Parse links
        if apply_url_timeout && Instant::now() >= url_deadline {
            tracing::warn!(worker_id = worker_id, url = %current_url, "Per-URL timeout exceeded before HTML parsing, skipping");
            continue;
        }
        let parse_handle = tokio::task::spawn_blocking(move || {
            let found_links = Arc::new(std::sync::Mutex::new(Vec::new()));
            let links_clone = Arc::clone(&found_links);
            let mut rewriter = HtmlRewriter::new(
                Settings {
                    element_content_handlers: vec![element!("a[href]", move |el| {
                        if let Some(href) = el.get_attribute("href") {
                            if let Ok(mut guard) = links_clone.lock() {
                                guard.push(href);
                            }
                        }
                        Ok(())
                    })],
                    ..Settings::default()
                },
                |_: &[u8]| {},
            );
            rewriter.write(&body).ok();
            rewriter.end().ok();
            found_links.lock().map(|g| g.clone()).unwrap_or_default()
        });
        let discovered_links = if apply_url_timeout {
            match await_result_with_deadline(url_deadline, parse_handle).await {
                Ok(links) => links,
                Err(DeadlineAwaitError::Timeout) => {
                    tracing::warn!(worker_id = worker_id, url = %current_url, "HTML parsing exceeded per-URL timeout, skipping");
                    continue;
                }
                Err(DeadlineAwaitError::Error(_)) => Vec::new(),
            }
        } else {
            parse_handle.await.unwrap_or_default()
        };

        // 4. Normalize and filter
        let mut new_valid_urls = Vec::new();
        for href in discovered_links {
            if let Ok(mut new_url) = current_url.join(href.trim()) {
                if !is_http_scheme(&new_url) {
                    continue;
                }
                normalize_url(&mut new_url);
                new_valid_urls.push(new_url.to_string());
            }
        }
        new_valid_urls.sort();
        new_valid_urls.dedup();

        // 5. Save results to S3
        if apply_url_timeout && Instant::now() >= url_deadline {
            tracing::warn!(worker_id = worker_id, url = %current_url, "Per-URL timeout exceeded before metadata upload, skipping");
            continue;
        }
        let result = CrawlResult {
            source_url: current_url.to_string(),
            depth: task.depth,
            scraped_at: chrono::Utc::now().to_rfc3339(),
            found_links: new_valid_urls.clone(),
        };
        let metadata_future = save_data_to_s3(&s3_client, &s3_bucket, &result);
        let metadata_result = if apply_url_timeout {
            match await_result_with_deadline(url_deadline, metadata_future).await {
                Ok(res) => Ok(res),
                Err(DeadlineAwaitError::Timeout) => {
                    tracing::warn!(worker_id = worker_id, url = %current_url, "Metadata upload exceeded per-URL timeout, skipping");
                    continue;
                }
                Err(DeadlineAwaitError::Error(e)) => Err(e),
            }
        } else {
            metadata_future.await
        };
        if let Err(e) = metadata_result {
            tracing::error!(worker_id = worker_id, error = ?e, url = %current_url, "Failed to save crawl metadata to S3");
        }

        // 6. Add new URLs to the queue, respecting max_depth
        let next_depth = task.depth + 1;
        if next_depth <= settings.max_depth && !new_valid_urls.is_empty() {
            if apply_url_timeout && Instant::now() >= url_deadline {
                tracing::warn!(worker_id = worker_id, url = %current_url, "Per-URL timeout exceeded before queueing discoveries, skipping");
                continue;
            }
            let new_tasks: Vec<String> = new_valid_urls
                .into_iter()
                .map(|url| {
                    let task = UrlTask {
                        url,
                        depth: next_depth,
                    };
                    serde_json::to_string(&task).unwrap_or_default()
                })
                .filter(|s| !s.is_empty())
                .collect();

            if new_tasks.is_empty() {
                continue;
            }

            let script = redis::Script::new(SCRIPT_ADD_IF_NOT_EXISTS);
            let urls_added: usize = script
                .key(visited_set_key.as_str())
                .key(url_queue_key.as_str())
                .arg(&new_tasks)
                .invoke_async(&mut redis_con)
                .await?;
            if urls_added > 0 {
                tracing::info!(
                    worker_id = worker_id,
                    added_count = urls_added,
                    next_depth,
                    source_url = %current_url,
                    "Discovered new URLs"
                );
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter("info")
        .init();

    let config_path =
        std::env::var("WORKER_CONFIG_PATH").unwrap_or_else(|_| "worker_config.toml".to_string());

    let settings = Arc::new(
        SettingsLoader::builder()
            .add_source(File::new(&config_path, FileFormat::Toml))
            .build()?
            .try_deserialize::<WorkerConfig>()?,
    );

    tracing::info!(
        tasks_to_spawn = settings.concurrent_tasks,
        max_depth = settings.max_depth,
        delay_ms = settings.politeness_delay_ms,
        "Loaded config, preparing to spawn worker tasks"
    );
    let mut tasks = tokio::task::JoinSet::new();
    for i in 1..=settings.concurrent_tasks {
        let task_settings = Arc::clone(&settings);
        tasks.spawn(async move {
            if let Err(e) = worker_loop(i, task_settings).await {
                tracing::error!(worker_id = i, error = ?e, "worker loop failed");
            }
        });
    }
    while let Some(res) = tasks.join_next().await {
        if let Err(e) = res {
            tracing::error!(error = ?e, "A worker task panicked or was cancelled");
        }
    }
    Ok(())
}
