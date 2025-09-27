use csv;
use redis::Commands;
use serde::Serialize;
use std::error::Error;
use url::Url;

#[derive(Serialize)]
struct UrlTask {
    url: String,
    depth: u32,
}

// Copied from your original file to read the seeds
#[derive(Clone, Debug)]
enum SeedColumn {
    Index(usize),
    Name(String),
}

fn read_seeds_csv(path: &str, col: Option<SeedColumn>) -> Result<Vec<Url>, Box<dyn Error>> {
    let mut rdr = csv::Reader::from_path(path)?;
    let mut idx: usize = 0;
    if let Some(spec) = col {
        match spec {
            SeedColumn::Index(i) => idx = i,
            SeedColumn::Name(name) => {
                if let Ok(headers) = rdr.headers() {
                    let target = name.to_ascii_lowercase();
                    if let Some(pos) = headers
                        .iter()
                        .position(|h| h.trim().eq_ignore_ascii_case(&target))
                    {
                        idx = pos;
                    } else {
                        eprintln!(
                            "Seed column '{}' not found. Falling back to first column.",
                            name
                        );
                    }
                } else {
                    eprintln!("No headers in seeds CSV. Falling back to first column.");
                }
            }
        }
    }

    let mut urls = Vec::new();
    for rec in rdr.records() {
        let rec = rec?;
        if let Some(raw) = rec.get(idx) {
            let s = raw.trim();
            if s.is_empty() {
                continue;
            }
            match Url::parse(s) {
                Ok(mut u) => {
                    u.set_fragment(None); // Normalize here
                    urls.push(u)
                }
                Err(_) => eprintln!("Skipping invalid URL in seeds CSV: {}", s),
            }
        }
    }
    if urls.is_empty() {
        Err("no seed URLs found in CSV".into())
    } else {
        Ok(urls)
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!(
            "Usage: seeder <path_to_seeds.csv> [--column-index <number> | --column-name <header>]"
        );
        return Err("Missing seed file argument".into());
    }

    let mut column_spec: Option<SeedColumn> = None;
    let mut idx = 2;
    while idx < args.len() {
        match args[idx].as_str() {
            "--column-index" => {
                let value = args
                    .get(idx + 1)
                    .ok_or("Missing value for --column-index")?;
                let parsed = value
                    .parse::<usize>()
                    .map_err(|_| "Invalid index for --column-index")?;
                column_spec = Some(SeedColumn::Index(parsed));
                idx += 2;
            }
            "--column-name" => {
                let value = args
                    .get(idx + 1)
                    .ok_or("Missing value for --column-name")?
                    .to_string();
                if value.trim().is_empty() {
                    return Err("Column name cannot be empty".into());
                }
                column_spec = Some(SeedColumn::Name(value));
                idx += 2;
            }
            other => {
                eprintln!("Unknown argument: {}", other);
                eprintln!(
                    "Usage: seeder <path_to_seeds.csv> [--column-index <number> | --column-name <header>]"
                );
                return Err("Invalid command line arguments".into());
            }
        }
    }

    println!("Reading seeds from '{}'...", &args[1]);
    let seeds = read_seeds_csv(&args[1], column_spec)?;

    // Create initial tasks with depth 0
    let initial_tasks: Vec<String> = seeds
        .iter()
        .map(|url| {
            let task = UrlTask {
                url: url.to_string(),
                depth: 0,
            };
            serde_json::to_string(&task).unwrap()
        })
        .collect();

    println!("Found {} valid seed URLs.", seeds.len());

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://100.123.122.55:6379/".to_string());
    let queue_key =
        std::env::var("URL_QUEUE_KEY").unwrap_or_else(|_| "crawler:url_queue".to_string());

    println!("Connecting to Redis at '{}'...", redis_url);
    let client = redis::Client::open(redis_url.as_str())?;
    let mut con = client.get_connection()?;

    // Push the JSON tasks to the queue
    println!(
        "Adding {} initial tasks (depth 0) to the URL queue...",
        initial_tasks.len()
    );
    let _: () = con.lpush(queue_key.as_str(), &initial_tasks)?;

    println!("\n✅ Success! Redis has been seeded.");

    Ok(())
}
