import openai
import psycopg2
import os
import json
import time
import pandas as pd
import re
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- CONFIGURATION ---

# Secrets (loaded from your .env file)
DB_PARAMS = {
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)), # Uses default port 5432 if not set
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD")
}
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- IMPORTANT: Define your benchmark Kommunen here (use lowercase, slugified names) ---
BENCHMARK_KOMMUNEN = ["muenchen", "berlin", "hamburg"] 

# File Paths
OFFICIAL_DATA_FOLDER = "./official_data" # For benchmark .txt files
RAW_HTML_FOLDER = "./raw_html"           # For all other raw .html files

# Other Settings
DB_QUERY = "SELECT name, partei, position, kommune FROM politicians_output;"
BATCH_MODEL = "gpt-4o"
BATCH_COMPLETION_WINDOW = "24h"
BATCH_INPUT_FILENAME = "batch_judge_requests.jsonl"
DETAILED_RESULTS_CSV = "validation_results_detailed.csv"
HTML_REPORT_FILENAME = "validation_report.html"


# --- PROMPT TEMPLATES ---

# 1. Prompt for comparing against clean .txt files (for the Benchmark set)
GERMAN_PROMPT_CLEAN_TXT = """
**ROLLE:**
Sie sind ein sehr sorgfältiger Datenvalidierungs-Experte mit tiefgreifenden Kenntnissen der deutschen Kommunalpolitik. Ihre Aufgabe ist die Qualitätskontrolle.

**KONTEXT:**
Sie erhalten zwei Datensätze:
1. `PIPELINE_OUTPUT`: Ein JSON-Objekt, das von einer KI-Pipeline extrahiert wurde.
2. `OFFICIAL_DATA`: Text, der von der offiziellen Webseite der jeweiligen Kommune extrahiert wurde. Dies ist die Referenzwahrheit ("Ground Truth").

Ihre Aufgabe ist es, den `PIPELINE_OUTPUT` mit den `OFFICIAL_DATA` zu vergleichen und die Korrektheit zu bewerten.

**ANWEISUNGEN:**
1.  **Person überprüfen:** Finden Sie die Person aus dem `PIPELIPELINE_OUTPUT` in den `OFFICIAL_DATA`. Seien Sie flexibel bei akademischen Titeln (z.B. Dr., Prof.) und leichten Namensabweichungen.
2.  **Partei überprüfen:** Wenn die Person gefunden wurde, bestätigen Sie, dass die politische Partei ("Partei" oder "Fraktion") übereinstimmt. Berücksichtigen Sie gängige Abkürzungen (CSU, SPD, GRÜNE) und offizielle Namen (Bündnis 90/Die Grünen).
3.  **Position überprüfen:** Bestätigen Sie, dass die Position ("Position") der Person übereinstimmt. Achten Sie auf Variationen wie "Erster Bürgermeister" vs. "Bürgermeister".

**ENTSCHEIDUNGSREGELN:**
* **PASS:** Das Urteil lautet "PASS", nur wenn Person, Partei und Position korrekt mit den `OFFICIAL_DATA` übereinstimmen.
* **FAIL:** Das Urteil lautet "FAIL", wenn die Person nicht gefunden werden kann oder wenn entweder die Partei oder die Position nicht übereinstimmt.

**AUSGABEFORMAT:**
Antworten Sie ausschließlich in einem sauberen JSON-Format ohne zusätzliche Kommentare. Das JSON-Objekt muss drei Schlüssel enthalten:
* `"verdict"`: Ihre endgültige Entscheidung, entweder "PASS" oder "FAIL".
* `"confidence"`: Ihr Vertrauen in das Urteil ("High", "Medium" oder "Low"). Verwenden Sie "Low" für mehrdeutige Fälle.
* `"reason"`: Eine kurze, klare Begründung für Ihre Entscheidung in einem Satz.

---
**GESTELLTE AUFGABE:**

**`PIPELINE_OUTPUT`:**
{pipeline_output}

**`OFFICIAL_DATA`:**
{official_data}

**IHRE ANTWORT:**
"""

# 2. Prompt for validating against raw HTML (for all other data)
GERMAN_PROMPT_RAW_HTML = """
**ROLLE:**
Sie sind ein sehr sorgfältiger Datenvalidierungs-Experte mit tiefgreifenden Kenntnissen der deutschen Kommunalpolitik. Ihre Aufgabe ist die Qualitätskontrolle.

**KONTEXT:**
Sie erhalten zwei Datensätze:
1. `PIPELINE_OUTPUT`: Ein JSON-Objekt, das von einer KI-Pipeline aus einem HTML-Dokument extrahiert wurde.
2. `RAW_HTML`: Das ursprüngliche, unformatierte HTML-Dokument.

Ihre Aufgabe ist es, das `RAW_HTML` zu analysieren und zu überprüfen, ob die Informationen im `PIPELINE_OUTPUT` korrekt sind. Sie agieren als Senior-Prüfer, der die Arbeit einer Junior-KI überprüft.

**ANWEISUNGEN:**
1.  **Person finden:** Durchsuchen Sie das `RAW_HTML`, um die Person aus dem `PIPELINE_OUTPUT` zu finden.
2.  **Kontext analysieren:** Wenn Sie die Person finden, analysieren Sie den umgebenden HTML-Text, um Partei und Position zu ermitteln.
3.  **Vergleichen und entscheiden:** Vergleichen Sie die im HTML gefundenen Informationen mit denen im `PIPELINE_OUTPUT`.

**ENTSCHEIDUNGSREGELN:**
* **PASS:** Das Urteil lautet "PASS", nur wenn Sie die Person im HTML finden und deren Partei und Position mit dem `PIPELINE_OUTPUT` übereinstimmen.
* **FAIL:** Das Urteil lautet "FAIL", wenn die Person nicht gefunden werden kann oder die Informationen zu Partei oder Position nicht übereinstimmen.

**AUSGABEFORMAT:**
Antworten Sie ausschließlich in einem sauberen JSON-Format ohne zusätzliche Kommentare.
{ "verdict": "...", "confidence": "...", "reason": "..." }

---
**GESTELLTE AUFGABE:**

**`PIPELINE_OUTPUT`:**
{pipeline_output}

**`RAW_HTML`:**
{raw_html}

**IHRE ANTWORT:**
"""

def slugify(text):
    """Converts a string like "München" to "muenchen" for filenames."""
    text = text.lower()
    text = text.replace('ü', 'ue').replace('ö', 'oe').replace('ä', 'ae').replace('ß', 'ss')
    text = re.sub(r'[^a-z0-9]+', '-', text).strip('-')
    return text

def fetch_pipeline_data(conn_params, query):
    """Fetches data from the PostgreSQL database."""
    print("Connecting to PostgreSQL to fetch pipeline data...")
    try:
        with psycopg2.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                data = [dict(zip(columns, row)) for row in cur.fetchall()]
                print(f"✅ Successfully fetched {len(data)} records from the database.")
                return data
    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return []

def prepare_batch_file(pipeline_data, official_data_folder, raw_html_folder, benchmark_list):
    """Prepares the JSONL file with two-phase logic."""
    print("Preparing batch input file with two-phase logic...")
    requests = []
    
    for record in pipeline_data:
        kommune_slug = slugify(record['kommune'])
        pipeline_output_json = json.dumps(record, ensure_ascii=False)
        prompt = ""
        
        # Phase 1: Use clean .txt file for benchmark Kommunen
        if kommune_slug in benchmark_list:
            data_path = os.path.join(official_data_folder, f"{kommune_slug}.txt")
            if os.path.exists(data_path):
                with open(data_path, 'r', encoding='utf-8') as f:
                    official_data = f.read()
                prompt = GERMAN_PROMPT_CLEAN_TXT.format(
                    pipeline_output=pipeline_output_json,
                    official_data=official_data
                )
            else:
                print(f"⚠️ Warning: Benchmark Kommune '{record['kommune']}' is missing its .txt file at {data_path}")
                continue

        # Phase 2: Use raw .html file for all other Kommunen
        else:
            data_path = os.path.join(raw_html_folder, f"{kommune_slug}.html") # Assuming .html extension
            if os.path.exists(data_path):
                with open(data_path, 'r', encoding='utf-8') as f:
                    raw_html_data = f.read()
                prompt = GERMAN_PROMPT_RAW_HTML.format(
                    pipeline_output=pipeline_output_json,
                    raw_html=raw_html_data
                )
            else:
                print(f"⚠️ Warning: No raw HTML file found for {record['kommune']} at {data_path}")
                continue
        
        requests.append({
            "custom_id": f"req_{record['kommune']}_{record['name']}".replace(" ", "_"),
            "method": "POST",
            "url": "/v1/chat/completions",
            "body": {
                "model": BATCH_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "response_format": {"type": "json_object"},
                "temperature": 0.0
            }
        })

    with open(BATCH_INPUT_FILENAME, 'w', encoding='utf-8') as f:
        for req in requests:
            f.write(json.dumps(req, ensure_ascii=False) + "\n")
            
    print(f"✅ Batch input file '{BATCH_INPUT_FILENAME}' created with {len(requests)} requests.")
    return BATCH_INPUT_FILENAME

def run_openai_batch_job(client, batch_input_file):
    """Creates, runs, and monitors an OpenAI batch job."""
    print("\n--- Starting OpenAI Batch Job ---")
    
    print(f"Uploading file '{batch_input_file}'...")
    batch_file = client.files.create(file=open(batch_input_file, "rb"), purpose="batch")
    print(f"File uploaded with ID: {batch_file.id}")

    print("Creating batch job...")
    batch_job = client.batches.create(
        input_file_id=batch_file.id,
        endpoint="/v1/chat/completions",
        completion_window=BATCH_COMPLETION_WINDOW
    )
    print(f"Batch job created with ID: {batch_job.id}")

    print("Waiting for batch job to complete... (this may take a while)")
    while batch_job.status not in ["completed", "failed", "cancelled"]:
        time.sleep(30)
        batch_job = client.batches.retrieve(batch_job.id)
        print(f"Current status: {batch_job.status} ({time.strftime('%H:%M:%S')})")

    if batch_job.status != "completed":
        print(f" Batch job did not complete successfully. Final status: {batch_job.status}")
        return None

    print(" Batch job completed!")
    
    output_file_id = batch_job.output_file_id
    print(f"Downloading results from file ID: {output_file_id}")
    result_content = client.files.content(output_file_id).content.decode('utf-8')
    return result_content

def generate_html_report(stats, confidence_df, reasons_df):
    """Generates a styled HTML report from the validation statistics."""
    report_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    stats_html = stats.to_html(index=False, classes='stats-table', border=0)
    confidence_html = confidence_df.to_html(index=False, classes='stats-table', border=0)
    reasons_html = reasons_df.to_html(index=False, classes='stats-table', border=0)

    html_style = """
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif; margin: 0 auto; max-width: 800px; padding: 2em; color: #333; }
        h1, h2 { color: #1a1a1a; border-bottom: 2px solid #007bff; padding-bottom: 10px; }
        .stats-table { border-collapse: collapse; width: 100%; margin-bottom: 2em; box-shadow: 0 2px 3px rgba(0,0,0,0.1); }
        .stats-table th, .stats-table td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #ddd; }
        .stats-table th { background-color: #007bff; color: white; }
        .stats-table tr:nth-child(even) { background-color: #f8f9fa; }
        .stats-table tr:hover { background-color: #e9ecef; }
        .footer { font-size: 0.8em; color: #777; margin-top: 2em; text-align: center; }
    </style>
    """
    
    html_content = f"""
    <!DOCTYPE html>
    <html lang="de">
    <head>
        <meta charset="UTF-8">
        <title>AI Judge Validation Report</title>
        {html_style}
    </head>
    <body>
        <h1>AI Judge Validation Report</h1>
        <p>Generated on: {report_date}</p>
        <h2>Overall Statistics</h2>
        {stats_html}
        <h2>Breakdown by Confidence</h2>
        {confidence_html}
        <h2>Top 5 Failure Reasons</h2>
        {reasons_html}
        <div class="footer">This report was automatically generated by the validation script.</div>
    </body>
    </html>
    """
    
    with open(HTML_REPORT_FILENAME, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"✅ HTML report saved to '{HTML_REPORT_FILENAME}'")

def analyze_results(result_content):
    """Parses the batch output, generates statistics, and creates an HTML report."""
    print("\n--- Analyzing Validation Results ---")
    if not result_content:
        print("No result content to analyze.")
        return

    parsed_results = []
    for line in result_content.strip().split('\n'):
        try:
            line_data = json.loads(line)
            custom_id = line_data.get('custom_id', 'unknown')
            response_body_str = line_data.get('response', {}).get('body', '{}')
            response_json_str = json.loads(response_body_str).get('choices', [{}])[0].get('message', {}).get('content', '{}')
            judge_output = json.loads(response_json_str)
            parsed_results.append({
                'custom_id': custom_id,
                'verdict': judge_output.get('verdict'),
                'confidence': judge_output.get('confidence'),
                'reason': judge_output.get('reason')
            })
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            print(f"⚠️ Warning: Could not parse a result line. Error: {e}. Line: {line[:100]}...")

    if not parsed_results:
        print(" No valid results were parsed.")
        return

    df = pd.DataFrame(parsed_results)
    df.to_csv(DETAILED_RESULTS_CSV, index=False)
    print(f"✅ Detailed results saved to '{DETAILED_RESULTS_CSV}'")
    
    total_records = len(df)
    pass_count = (df['verdict'] == 'PASS').sum()
    fail_count = (df['verdict'] == 'FAIL').sum()
    pass_rate = (pass_count / total_records) * 100 if total_records > 0 else 0
    
    stats_summary = pd.DataFrame({
        "Metric": ["Total Validated Records", "PASS", "FAIL", "Pass Rate (%)"],
        "Value": [total_records, pass_count, fail_count, f"{pass_rate:.2f}"]
    })
    
    confidence_summary = df['confidence'].value_counts().reset_index()
    confidence_summary.columns = ['Confidence Level', 'Count']

    fail_reasons_summary = df[df['verdict'] == 'FAIL']['reason'].value_counts().nlargest(5).reset_index()
    fail_reasons_summary.columns = ['Reason', 'Count']
    
    print("\n--- Validation Statistics ---")
    print(stats_summary.to_string(index=False))
    print("\nBreakdown by Confidence:")
    print(confidence_summary.to_string(index=False))
    print("\nTop 5 Failure Reasons:")
    print(fail_reasons_summary.to_string(index=False))
    
    generate_html_report(stats_summary, confidence_summary, fail_reasons_summary)

def main():
    """Main function to orchestrate the validation workflow."""
    if not OPENAI_API_KEY:
        print(" CRITICAL: OPENAI_API_KEY is not set in your .env file. Exiting.")
        return
        
    try:
        client = openai.OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print(f" Failed to initialize OpenAI client. Error: {e}")
        return

    pipeline_data = fetch_pipeline_data(DB_PARAMS, DB_QUERY)
    if not pipeline_data:
        print("No data fetched from the database. Exiting.")
        return
        
    batch_input_file = prepare_batch_file(pipeline_data, OFFICIAL_DATA_FOLDER, RAW_HTML_FOLDER, BENCHMARK_KOMMUNEN)
    
    if os.path.getsize(batch_input_file) == 0:
        print(" Batch input file is empty. This likely means no source files were found. Exiting.")
        return

    result_content = run_openai_batch_job(client, batch_input_file)
    
    if result_content:
        analyze_results(result_content)

if __name__ == "__main__":
    main()