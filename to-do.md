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

    setup_redis_logging_fixed()  # ← FIXED: Use new thread-safe version
    
    # Start metrics reporter
    start_metrics_reporter(60)  # Log metrics every 60 seconds

    @info "Worker started." queue=CONFIG.REDIS_QUEUE_NAME threads=nthreads()

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
        increment_metric!(METRICS, :batches_processed)

        json_lines = Vector{Union{Nothing,String}}(undef, length(batch))
        for i in eachindex(json_lines)
            json_lines[i] = nothing
        end

        @threads for i in eachindex(batch)
            source_key = batch[i]
            increment_metric!(METRICS, :jobs_processed)
            
            try
                @info "Processing job." source_key=source_key thread_id=Threads.threadid()

                @info "Downloading from S3." bucket=CONFIG.S3_SOURCE_BUCKET key=source_key
                html_content = try
                    download_html(CONFIG.S3_SOURCE_BUCKET, source_key)
                catch err
                    increment_metric!(METRICS, :s3_download_failures)
                    increment_metric!(METRICS, :jobs_failed)
                    @error "Failed to download source object." bucket=CONFIG.S3_SOURCE_BUCKET key=source_key exception=(err, catch_backtrace())
                    json_lines[i] = nothing
                    continue
                end

                increment_metric!(METRICS, :html_bytes_processed, length(html_content))

                cleaned_html = try
                    clean_html_content(html_content)
                catch err
                    increment_metric!(METRICS, :html_cleaning_failures)
                    increment_metric!(METRICS, :jobs_failed)
                    @error "HTML cleaning failed, using original." key=source_key exception=(err, catch_backtrace())
                    html_content  # Fallback to original
                end

                @info "Uploading cleaned HTML to S3." bucket=CONFIG.S3_DESTINATION_BUCKET key=source_key
                try
                    upload_object(CONFIG.S3_DESTINATION_BUCKET, source_key, cleaned_html, "text/html; charset=utf-8")
                    @info "Upload complete." key=source_key
                catch err
                    increment_metric!(METRICS, :s3_upload_failures)
                    increment_metric!(METRICS, :jobs_failed)
                    @error "Failed to upload cleaned HTML." bucket=CONFIG.S3_DESTINATION_BUCKET key=source_key exception=(err, catch_backtrace())
                    json_lines[i] = nothing
                    continue
                end

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
                        "temperature" => CONFIG.OPENAI_TEMPERATURE,
                    ),
                    "metadata" => Dict(
                        "source_bucket" => CONFIG.S3_SOURCE_BUCKET,
                        "source_key" => source_key,
                        "cleaned_bucket" => CONFIG.S3_DESTINATION_BUCKET,
                        "cleaned_key" => source_key,
                        "jsonl_bucket" => CONFIG.S3_JSONL_BUCKET,
                    ),
                )

                json_lines[i] = JSON.json(request_obj; canonical=true)
                increment_metric!(METRICS, :jobs_succeeded)
                @info "Prepared JSONL entry." key=source_key custom_id=custom_id
            catch e
                json_lines[i] = nothing
                increment_metric!(METRICS, :jobs_failed)
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
            increment_metric!(METRICS, :jsonl_files_uploaded)
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