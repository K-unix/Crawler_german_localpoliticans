using Gumbo
using AbstractTrees
using Redis
using AWS
using AWSS3
using HTTP
using SHA
using Printf
using Logging
using Dates
using Base.Threads
using Base: basename
using JSON
using Random

const CONSOLE_LOGGER = ConsoleLogger(stdout)
global_logger(CONSOLE_LOGGER)
const REDIS_LOGGING_INITIALIZED = Ref(false)

# --- CONFIGURATION LOADER ---
function get_env_var(key::String, default::String="")::String
    val = get(ENV, key, default)
    if isempty(val) && isempty(default)
        @warn "Environment variable '$key' is not set."
    end
    return val
end

@info "Loading configuration from environment variables..."
const CONFIG = (
    S3_SOURCE_BUCKET      = get_env_var("S3_SOURCE_BUCKET"),
    S3_DESTINATION_BUCKET = get_env_var("S3_DESTINATION_BUCKET"),
    S3_JSONL_BUCKET       = begin
        dest = get_env_var("S3_DESTINATION_BUCKET")
        get_env_var("S3_JSONL_BUCKET", dest)
    end,
    AWS_REGION            = get_env_var("AWS_REGION", "us-east-1"),
    AWS_ACCESS_KEY_ID     = get_env_var("AWS_ACCESS_KEY_ID"),
    AWS_SECRET_ACCESS_KEY = get_env_var("AWS_SECRET_ACCESS_KEY"),
    AWS_SESSION_TOKEN     = get_env_var("AWS_SESSION_TOKEN", ""),
    AWS_ENDPOINT_URL      = get_env_var("AWS_ENDPOINT_URL", ""),
    AWS_ALLOW_HTTP        = get_env_var("AWS_ALLOW_HTTP", ""),
    REDIS_HOST            = get_env_var("REDIS_HOST", "redis"),
    REDIS_PORT            = parse(Int, get_env_var("REDIS_PORT", "6379")),
    REDIS_QUEUE_NAME      = get_env_var("REDIS_QUEUE_NAME", "html_processing_queue"),
    REDIS_LOG_LIST        = get_env_var("REDIS_LOG_LIST", ""),
    OPENAI_MODEL          = get_env_var("OPENAI_MODEL", "gpt-4-turbo"),
    OPENAI_API_ENDPOINT   = get_env_var("OPENAI_API_ENDPOINT", "/v1/chat/completions")
)

const BATCH_SIZE = 20
const OPENAI_SYSTEM_MESSAGE = "You are a helpful assistant that analyzes HTML content."
const OPENAI_USER_PREFIX = "Analyze the following HTML document: \n\n"

@info "Julia threading configured." threads=nthreads()

truthy(value::String) = lowercase(strip(value)) in ("1", "true", "yes", "on")

struct MinioClient
    uri::HTTP.URI
    host_header::String
    region::String
    access_key::String
    secret_key::String
    session_token::Union{Nothing,String}
end

const EMPTY_PAYLOAD_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

default_port(scheme::AbstractString) = scheme == "https" ? 443 : 80

function build_minio_client(endpoint::String, region::String, access_key::String, secret_key::String, session_token::Union{Nothing,String}; allow_http::Bool)
    isempty(endpoint) && error("Endpoint must be provided for MinIO client")
    uri = HTTP.URI(endpoint)
    scheme = isempty(uri.scheme) ? (allow_http ? "http" : "https") : lowercase(uri.scheme)
    if scheme == "http" && !allow_http
        error("HTTP endpoint '$endpoint' not permitted. Set AWS_ALLOW_HTTP=1 to enable.")
    end
    port = uri.port == 0 ? default_port(scheme) : uri.port
    host_header = port == default_port(scheme) ? uri.host : string(uri.host, ":", port)
    normalized_path = isempty(uri.path) ? "" : uri.path
    normalized_uri = HTTP.URI(; scheme=scheme, host=uri.host, port=port, path=normalized_path)
    MinioClient(normalized_uri, host_header, region, access_key, secret_key, session_token)
end

@inline function s3_safe_char(byte::UInt8)
    (byte >= 0x30 && byte <= 0x39) || # 0-9
    (byte >= 0x41 && byte <= 0x5A) || # A-Z
    (byte >= 0x61 && byte <= 0x7A) || # a-z
    byte in (0x2D, 0x2E, 0x5F, 0x7E, 0x2F) # - . _ ~ /
end

function s3_escape(path::AbstractString)
    io = IOBuffer()
    for byte in codeunits(path)
        if s3_safe_char(byte)
            write(io, byte)
        else
            @printf(io, "%%%02X", byte)
        end
    end
    String(take!(io))
end

function canonical_s3_path(bucket::AbstractString, key::AbstractString)
    bucket_enc = s3_escape(bucket)
    if isempty(key)
        return string('/', bucket_enc)
    end
    key_segments = split(key, '/'; keepempty=true)
    encoded_segments = map(s3_escape, key_segments)
    return string('/', bucket_enc, '/', join(encoded_segments, '/'))
end

sha256_hex(data::Vector{UInt8}) = bytes2hex(sha256(data))
sha256_hex(data::AbstractString) = sha256_hex(Vector{UInt8}(codeunits(data)))

function hmac_sha256_bytes(key::Vector{UInt8}, data::AbstractString)
    hmac_sha256(key, Vector{UInt8}(codeunits(data)))
end

function derive_signing_key(secret_key::AbstractString, date_stamp::AbstractString, region::AbstractString)
    k_secret = Vector{UInt8}(codeunits("AWS4" * secret_key))
    k_date = hmac_sha256_bytes(k_secret, date_stamp)
    k_region = hmac_sha256_bytes(k_date, region)
    k_service = hmac_sha256_bytes(k_region, "s3")
    hmac_sha256_bytes(k_service, "aws4_request")
end

function minio_signed_request(client::MinioClient, method::AbstractString, bucket::AbstractString, key::AbstractString; body::Vector{UInt8}=UInt8[], content_type::Union{Nothing,String}=nothing)
    now_utc = Dates.now(Dates.UTC)
    amz_date = Dates.format(now_utc, dateformat"yyyymmdd\THHMMSS\Z")
    date_stamp = Dates.format(now_utc, dateformat"yyyymmdd")

    canonical_path = canonical_s3_path(bucket, key)
    base_path = client.uri.path
    base_prefix = isempty(base_path) || base_path == "/" ? "" : rstrip(base_path, '/')
    full_path = string(base_prefix, canonical_path)
    full_path = isempty(full_path) ? "/" : full_path

    payload_hash = isempty(body) ? EMPTY_PAYLOAD_SHA256 : sha256_hex(body)

    header_pairs = Vector{Pair{String,String}}()
    push!(header_pairs, "host" => client.host_header)
    push!(header_pairs, "x-amz-content-sha256" => payload_hash)
    push!(header_pairs, "x-amz-date" => amz_date)
    if client.session_token !== nothing
        push!(header_pairs, "x-amz-security-token" => client.session_token::String)
    end
    if content_type !== nothing
        push!(header_pairs, "content-type" => content_type)
    end
    if method == "PUT"
        push!(header_pairs, "content-length" => string(length(body)))
    end

    sorted_headers = Base.sort(header_pairs; by = first)
    canonical_headers = IOBuffer()
    for (name, value) in sorted_headers
        write(canonical_headers, lowercase(name))
        write(canonical_headers, ':')
        write(canonical_headers, strip(value))
        write(canonical_headers, '\n')
    end
    canonical_headers_str = String(take!(canonical_headers))
    signed_headers = join(map(x -> lowercase(first(x)), sorted_headers), ';')

    canonical_request = join((
        uppercase(method),
        full_path,
        "",
        canonical_headers_str,
        signed_headers,
        payload_hash
    ), '\n')

    hashed_canonical_request = sha256_hex(canonical_request)
    credential_scope = string(date_stamp, '/', client.region, "/s3/aws4_request")
    string_to_sign = join((
        "AWS4-HMAC-SHA256",
        amz_date,
        credential_scope,
        hashed_canonical_request
    ), '\n')

    signing_key = derive_signing_key(client.secret_key, date_stamp, client.region)
    signature = bytes2hex(hmac_sha256_bytes(signing_key, string_to_sign))

    authorization = string(
        "AWS4-HMAC-SHA256 Credential=",
        client.access_key,
        '/',
        credential_scope,
        ", SignedHeaders=",
        signed_headers,
        ", Signature=",
        signature,
    )

    headers = HTTP.Headers(vcat(sorted_headers, ["authorization" => authorization]))

    request_uri = HTTP.URI(; scheme=client.uri.scheme, host=client.uri.host, port=client.uri.port, path=full_path)
    response = HTTP.request(method, request_uri, headers, body)
    if response.status >= 200 && response.status < 300
        return response
    end
    throw(ErrorException("S3 request failed with status $(response.status): $(String(response.body))"))
end

function minio_get_object(client::MinioClient, bucket::AbstractString, key::AbstractString)
    response = minio_signed_request(client, "GET", bucket, key)
    response.body
end

function minio_put_object(client::MinioClient, bucket::AbstractString, key::AbstractString, body::AbstractString; content_type::String="application/octet-stream")
    bytes = Vector{UInt8}(codeunits(body))
    minio_signed_request(client, "PUT", bucket, key; body=bytes, content_type=content_type)
    nothing
end

struct RedisListLogger <: AbstractLogger
    conn::Redis.RedisConnection
    list_name::String
    min_level::LogLevel
end

struct CompositeLogger{L1<:AbstractLogger,L2<:AbstractLogger} <: AbstractLogger
    primary::L1
    secondary::L2
end

Logging.min_enabled_level(logger::RedisListLogger) = logger.min_level
Logging.shouldlog(logger::RedisListLogger, level, _module, group, id) = level >= logger.min_level
Logging.catch_exceptions(::RedisListLogger) = true

function Logging.handle_message(logger::RedisListLogger, level, message, _module, group, id, file, line; kwargs...)
    msg_body = message isa Function ? message() : message
    level_str = string(level)
    timestamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyy-mm-ddTHH:MM:SS.sssZ")
    meta_pairs = String[]
    for (k, v) in kwargs
        push!(meta_pairs, string(k, "=", repr(v)))
    end
    if file !== nothing && line !== nothing
        push!(meta_pairs, string("location=", file, ":", line))
    end
    if group !== nothing
        push!(meta_pairs, string("group=", group))
    end
    entry = isempty(meta_pairs) ?
        string(timestamp, " [", level_str, "] ", msg_body) :
        string(timestamp, " [", level_str, "] ", msg_body, " | ", join(meta_pairs, " "))
    try
        Redis.rpush(logger.conn, logger.list_name, entry)
    catch err
        # Swallow logging errors but surface them on stderr for visibility.
        Base.println(stderr, "Redis logging failure: ", err)
    end
end

Logging.min_enabled_level(logger::CompositeLogger) = min(Logging.min_enabled_level(logger.primary), Logging.min_enabled_level(logger.secondary))
function Logging.shouldlog(logger::CompositeLogger, level, _module, group, id)
    Logging.shouldlog(logger.primary, level, _module, group, id) || Logging.shouldlog(logger.secondary, level, _module, group, id)
end
Logging.catch_exceptions(logger::CompositeLogger) = Logging.catch_exceptions(logger.primary) && Logging.catch_exceptions(logger.secondary)

function Logging.handle_message(logger::CompositeLogger, level, message, _module, group, id, file, line; kwargs...)
    if Logging.shouldlog(logger.primary, level, _module, group, id)
        Logging.handle_message(logger.primary, level, message, _module, group, id, file, line; kwargs...)
    end
    if Logging.shouldlog(logger.secondary, level, _module, group, id)
        Logging.handle_message(logger.secondary, level, message, _module, group, id, file, line; kwargs...)
    end
end

function setup_redis_logging()
    isempty(CONFIG.REDIS_LOG_LIST) && return
    REDIS_LOGGING_INITIALIZED[] && return
    try
        logger_conn = Redis.RedisConnection(host=CONFIG.REDIS_HOST, port=CONFIG.REDIS_PORT)
        redis_logger = RedisListLogger(logger_conn, CONFIG.REDIS_LOG_LIST, Logging.Info)
        global_logger(CompositeLogger(CONSOLE_LOGGER, redis_logger))
        REDIS_LOGGING_INITIALIZED[] = true
        @info "Redis logging enabled" list=CONFIG.REDIS_LOG_LIST
    catch err
        @warn "Failed to initialize Redis logging; continuing with console logging only." exception=(err, catch_backtrace())
    end
end

# --- CORE CLEANING FUNCTION ---
function clean_html_content(html_content::String)::String
    try
        parsed_html = Gumbo.parsehtml(html_content)
        nodes_to_remove = HTMLElement[]
        for elem in PreOrderDFS(parsed_html.root)
            if elem isa HTMLElement && tag(elem) == :script
                push!(nodes_to_remove, elem)
            end
        end

        if !isempty(nodes_to_remove)
            scripts_removed = length(nodes_to_remove)
            @info "Removing <script> tag(s)." count=scripts_removed
            for node in nodes_to_remove
                if node.parent !== nothing
                    p = node.parent
                    idx = findfirst(isequal(node), p.children)
                    if idx !== nothing
                        deleteat!(p.children, idx)
                    end
                end
            end
        end
        return string(parsed_html.root)
    catch e
        @error "Could not parse or clean HTML. Returning original." exception=(e, catch_backtrace())
        return html_content
    end
end

function read_body_bytes(body)::Vector{UInt8}
    if body isa AbstractVector{UInt8}
        return Vector{UInt8}(body)
    end

    data = if hasmethod(take!, Tuple{typeof(body)})
        take!(body)
    elseif hasmethod(read, Tuple{typeof(body)})
        read(body)
    else
        error("Unsupported body type $(typeof(body)) for byte conversion")
    end

    if hasmethod(close, Tuple{typeof(body)})
        try
            close(body)
        catch
            # best effort close; ignore failures
        end
    end

    if data isa AbstractVector{UInt8}
        return Vector{UInt8}(data)
    elseif data isa AbstractString
        return Vector{UInt8}(codeunits(data))
    end

    return Vector{UInt8}(codeunits(String(data)))
end

function object_to_string(obj)::String
    if obj isa String
        return obj
    elseif obj isa AbstractVector{UInt8}
        return String(Vector{UInt8}(obj))
    elseif hasproperty(obj, :body)
        return String(read_body_bytes(getproperty(obj, :body)))
    elseif hasproperty(obj, :data)
        return String(read_body_bytes(getproperty(obj, :data)))
    end
    error("Unsupported object type $(typeof(obj)) for string conversion")
end

function derive_custom_id(source_key::AbstractString)::String
    filename = basename(source_key)
    candidate = isempty(filename) ? source_key : filename
    sanitized = replace(candidate, r"[^0-9A-Za-z_.-]" => "_")
    sanitized = strip(sanitized, '_')
    isempty(sanitized) && (sanitized = "file")
    return string("request_", sanitized)
end

# --- MAIN WORKER FUNCTION ---
function process_queue()
    if isempty(CONFIG.S3_SOURCE_BUCKET) || isempty(CONFIG.S3_DESTINATION_BUCKET) || isempty(CONFIG.S3_JSONL_BUCKET) || isempty(CONFIG.AWS_ACCESS_KEY_ID)
        @error "Essential S3 configuration is missing."
        return
    end

    access_key = CONFIG.AWS_ACCESS_KEY_ID
    secret_key = CONFIG.AWS_SECRET_ACCESS_KEY
    session_token = isempty(CONFIG.AWS_SESSION_TOKEN) ? nothing : CONFIG.AWS_SESSION_TOKEN
    endpoint = String(strip(CONFIG.AWS_ENDPOINT_URL))
    allow_http = truthy(CONFIG.AWS_ALLOW_HTTP) || (!isempty(endpoint) && startswith(lowercase(endpoint), "http://"))

    download_html = nothing
    upload_object = nothing

    if isempty(endpoint)
        creds = session_token === nothing ? AWSCredentials(access_key, secret_key) : AWSCredentials(access_key, secret_key, session_token)
        aws_config = global_aws_config(creds=creds, region=CONFIG.AWS_REGION)
        download_html = (bucket::String, key::String) -> begin
            response = s3_get(bucket, key; aws_config=aws_config)
            object_to_string(response)
        end
        upload_object = (bucket::String, key::String, content::String, content_type::String) -> begin
            s3_put(bucket, key, content; aws_config=aws_config, content_type=content_type)
            nothing
        end
        @info "Using AWS S3 backend"
    else
        minio_client = try
            build_minio_client(endpoint, CONFIG.AWS_REGION, access_key, secret_key, session_token; allow_http=allow_http)
        catch err
            @error "Failed to initialise custom S3 endpoint" endpoint=endpoint exception=(err, catch_backtrace())
            return
        end
        download_html = (bucket::String, key::String) -> begin
            bytes = minio_get_object(minio_client, bucket, key)
            object_to_string(bytes)
        end
        upload_object = (bucket::String, key::String, content::String, content_type::String) -> begin
            minio_put_object(minio_client, bucket, key, content; content_type=content_type)
            nothing
        end
        @info "Using custom S3 endpoint" endpoint=endpoint host=minio_client.host_header
    end

    @info "Attempting to connect to Redis..." host=CONFIG.REDIS_HOST port=CONFIG.REDIS_PORT
    redis_conn = nothing
    while redis_conn === nothing
        try
            redis_conn = Redis.RedisConnection(host=CONFIG.REDIS_HOST, port=CONFIG.REDIS_PORT)
        catch e
            @error "Failed to connect to Redis. Retrying in 5 seconds..." exception=(e, catch_backtrace()) host=CONFIG.REDIS_HOST port=CONFIG.REDIS_PORT
            sleep(5)
        end
    end

    setup_redis_logging()

    @info "Worker started." queue=CONFIG.REDIS_QUEUE_NAME

    queue_name = CONFIG.REDIS_QUEUE_NAME

    while true
        batch = String[]
        try
            _, first_key = Redis.brpop(redis_conn, [queue_name], 0)
            push!(batch, first_key)
            while length(batch) < BATCH_SIZE
                next_key = Redis.rpop(redis_conn, queue_name)
                next_key === nothing && break
                push!(batch, next_key)
            end
        catch err
            @error "Failed to retrieve batch from queue." queue=queue_name exception=(err, catch_backtrace())
            sleep(1)
            continue
        end

        @info "Processing batch." batch_size=length(batch)

        json_lines = Vector{Union{Nothing,String}}(undef, length(batch))
        for i in eachindex(json_lines)
            json_lines[i] = nothing
        end

        @threads for i in eachindex(batch)
            source_key = batch[i]
            try
                @info "Processing job." source_key=source_key

                @info "Downloading from S3." bucket=CONFIG.S3_SOURCE_BUCKET key=source_key
                html_content = try
                    download_html(CONFIG.S3_SOURCE_BUCKET, source_key)
                catch err
                    @error "Failed to download source object." bucket=CONFIG.S3_SOURCE_BUCKET key=source_key exception=(err, catch_backtrace())
                    json_lines[i] = nothing
                    continue
                end

                cleaned_html = clean_html_content(html_content)

                custom_id = derive_custom_id(source_key)
                user_message = string(OPENAI_USER_PREFIX, cleaned_html)

                request_obj = Dict(
                    "custom_id" => custom_id,
                    "method" => "POST",
                    "url" => CONFIG.OPENAI_API_ENDPOINT,
                    "body" => Dict(
                        "model" => CONFIG.OPENAI_MODEL,
                        "messages" => Any[
                            Dict("role" => "system", "content" => OPENAI_SYSTEM_MESSAGE),
                            Dict("role" => "user", "content" => user_message)
                        ],
                        "max_tokens" => 2048,
                    ),
                    "metadata" => Dict(
                        "source_bucket" => CONFIG.S3_SOURCE_BUCKET,
                        "source_key" => source_key,
                        "cleaned_bucket" => CONFIG.S3_DESTINATION_BUCKET,
                        "jsonl_bucket" => CONFIG.S3_JSONL_BUCKET,
                    ),
                )

                json_lines[i] = JSON.json(request_obj; canonical=true)
                @info "Prepared JSONL entry." key=source_key custom_id=custom_id
            catch e
                json_lines[i] = nothing
                @error "An error occurred during job processing." key=source_key exception=(e, catch_backtrace())
            end
        end

        valid_lines = String[]
        for line in json_lines
            if line !== nothing
                push!(valid_lines, line::String)
            end
        end

        if isempty(valid_lines)
            @warn "No valid JSON lines produced for batch; skipping upload." batch_size=length(batch)
            @info "Waiting for next job..." batch_size=length(batch)
            continue
        end

        timestamp = Dates.format(Dates.now(Dates.UTC), dateformat"yyyymmddHHMMSS")
        random_suffix = randstring(6)
        filename = string("batch_", timestamp, "_", random_suffix, ".jsonl")
        object_key = string("openai_batches/", filename)
        temp_path = tempname()

        try
            open(temp_path, "w") do io
                for line in valid_lines
                    write(io, line)
                    write(io, '\n')
                end
            end

            jsonl_content = read(temp_path, String)
            upload_object(CONFIG.S3_JSONL_BUCKET, object_key, jsonl_content, "application/json")
            @info "Uploaded batch JSONL." bucket=CONFIG.S3_JSONL_BUCKET key=object_key lines=length(valid_lines)
        catch err
            @error "Failed to upload JSONL batch." bucket=CONFIG.S3_JSONL_BUCKET key=object_key exception=(err, catch_backtrace())
        finally
            try
                rm(temp_path; force=true)
            catch cleanup_err
                @warn "Failed to remove temporary file." path=temp_path exception=(cleanup_err, catch_backtrace())
            end
        end

        @info "Waiting for next job..." batch_size=length(batch)
    end
end

# --- SCRIPT START ---
if abspath(PROGRAM_FILE) == @__FILE__
    process_queue()
end
