"""
Streaming Daily Pipeline - Memory-safe, resumable, fault-tolerant
Moved from scripts/streaming_daily_pipeline.py so that the whole project lives under the
`pipeline` package. No external sys.path hacks required.
"""

import json
import os
import time
import uuid
import tempfile
from datetime import datetime, timezone, date
from pathlib import Path
from typing import List, Dict, Any, Tuple
import random
import traceback
import warnings

import pandas as pd
import keepa
import requests
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage, bigquery
from google.cloud.exceptions import NotFound
from tqdm import tqdm

from pipeline.config import (
    GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_ID,
    KEEPA_API_KEY, TARGET_COLUMNS,
)

warnings.simplefilter(action="ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BATCH_SIZE = 50           # Reduced batch size to match token refill rate
FLUSH_INTERVAL = 5_000    # rows per Parquet file (~1-2 MB)
MAX_RETRIES = 3           # max retries per batch
INITIAL_BACKOFF = 1.0     # seconds
GCS_BUCKET = f"{GCP_PROJECT_ID}-keepa-staging"
STATE_BLOB = "daily_pipeline/state.json"

DOMAIN_MAPPING = {
    "AmazonUS": (1, "USD"),
    "AmazonGB": (3, "GBP"),
    "AmazonDE": (4, "EUR"),
    "AmazonJP": (6, "JPY"),
}

BQ_SCHEMA = pa.schema([
    pa.field("date", pa.date32()),
    pa.field("retail_price", pa.float64()),
    pa.field("discounted_price", pa.float64()),
    pa.field("rating", pa.float64()),
    pa.field("asin", pa.string()),
    pa.field("marketplace", pa.string()),
    pa.field("category", pa.string()),
    pa.field("created_at", pa.timestamp("us", tz="UTC")),
    pa.field("ingestion_date", pa.date32()),
])

# ---------------------------------------------------------------------------
# Helper classes
# ---------------------------------------------------------------------------
class StreamingParquetWriter:
    """Memory-safe streaming writer with automatic GCS upload"""

    def __init__(self, gcs_client: storage.Client, bucket_name: str, date_prefix: str):
        self.gcs_client = gcs_client
        self.bucket_name = bucket_name
        self.date_prefix = date_prefix
        self.writer = None
        self.tmp_fd = None
        self.tmp_path = None
        self.rows_in_file = 0
        self.uploaded_uris: List[str] = []

    # --------------------------------------
    # internal helpers
    # --------------------------------------
    def _create_new_file(self):
        if self.writer:
            self.writer.close()
        self.tmp_fd, self.tmp_path = tempfile.mkstemp(suffix=".parquet")
        self.writer = pq.ParquetWriter(self.tmp_path, BQ_SCHEMA)
        self.rows_in_file = 0

    # --------------------------------------
    # public API
    # --------------------------------------
    def write_batch(self, rows: List[Dict[str, Any]]):
        if not rows:
            return
        if self.writer is None:
            self._create_new_file()
        table = pa.Table.from_pylist(rows, schema=BQ_SCHEMA)
        self.writer.write_table(table)
        self.rows_in_file += len(rows)
        if self.rows_in_file >= FLUSH_INTERVAL:
            self.flush_and_rotate()

    def flush_and_rotate(self) -> str | None:
        if not self.writer or self.rows_in_file == 0:
            return None
        self.writer.close()
        os.close(self.tmp_fd)
        file_uuid = str(uuid.uuid4())
        gcs_path = f"{self.date_prefix}/{file_uuid}.parquet"
        gcs_uri = f"gs://{self.bucket_name}/{gcs_path}"
        bucket = self.gcs_client.bucket(self.bucket_name)
        bucket.blob(gcs_path).upload_from_filename(self.tmp_path)
        os.remove(self.tmp_path)
        self.uploaded_uris.append(gcs_uri)
        print(f"    📤 Uploaded {self.rows_in_file} rows → {gcs_uri}")
        # reset
        self.writer = None
        self.tmp_fd = None
        self.tmp_path = None
        self.rows_in_file = 0
        return gcs_uri

    def close(self) -> List[str]:
        if self.writer and self.rows_in_file > 0:
            self.flush_and_rotate()
        return self.uploaded_uris


class CheckpointManager:
    """Stores resume state in GCS"""

    def __init__(self, gcs_client: storage.Client, bucket_name: str, state_path: str):
        self.bucket = gcs_client.bucket(bucket_name)
        self.state_path = state_path

    def load_state(self) -> Dict[str, Any]:
        try:
            return json.loads(self.bucket.blob(self.state_path).download_as_text())
        except NotFound:
            return {"batch_offset": 0}

    def save_state(self, state: Dict[str, Any]):
        self.bucket.blob(self.state_path).upload_from_string(json.dumps(state))

    def clear_state(self):
        try:
            self.bucket.blob(self.state_path).delete()
        except NotFound:
            pass


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def exponential_backoff(attempt: int, base_delay: float = INITIAL_BACKOFF):
    delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
    print(f"    ⏳ Backing off {delay:.1f}s (attempt {attempt + 1})")
    time.sleep(delay)


def get_fx_rates() -> Dict[str, float]:
    try:
        rates = requests.get("https://api.exchangerate-api.com/v4/latest/USD").json()["rates"]
        return {
            "USD": 1.0,
            "GBP": 1 / rates["GBP"],
            "EUR": 1 / rates["EUR"],
            "JPY": 1 / rates["JPY"],
        }
    except Exception as exc:  # pragma: no cover — network call
        print(f"⚠️ FX rate fetch failed: {exc}. Using fallback rates")
        return {"USD": 1.0, "GBP": 1.25, "EUR": 1.1, "JPY": 0.007}


def prepare_batch_list(asin_data: Dict[str, Dict[str, List[str]]]) -> List[Tuple[str, str, List[str]]]:
    batches: List[Tuple[str, str, List[str]]] = []
    for domain_key, categories in asin_data.items():
        if domain_key not in DOMAIN_MAPPING:
            continue
        marketplace = domain_key.replace("Amazon", "")
        for category, asin_list in categories.items():
            for i in range(0, len(asin_list), BATCH_SIZE):
                batches.append((marketplace, category, asin_list[i : i + BATCH_SIZE]))
    return batches


def rows_from_products(products: List[Dict], marketplace: str, category: str, fx_rate: float) -> List[Dict[str, Any]]:
    today = date.today()
    created_ts = datetime.now(timezone.utc)
    rows: List[Dict[str, Any]] = []
    for p in products:
        stats = p.get("stats", {})
        current = stats.get("current", [])
        retail = current[3] / 100 if len(current) > 3 and current[3] > 0 else None
        discount = current[0] / 100 if len(current) > 0 and current[0] > 0 else None
        rating = None
        r_list = stats.get("rating")
        if isinstance(r_list, list) and r_list:
            rating = r_list[-1]
        rows.append(
            {
                "date": today,
                "retail_price": retail * fx_rate if retail else None,
                "discounted_price": discount * fx_rate if discount else None,
                "rating": rating,
                "asin": p["asin"],
                "marketplace": marketplace,
                "category": category,
                "created_at": created_ts,
                "ingestion_date": today,
            }
        )
    return rows


def fetch_batch_with_retry(api: keepa.Keepa, asin_batch: List[str], domain_id: str) -> List[Dict]:
    # Use direct HTTP GET instead of Keepa client query
    url = "https://api.keepa.com/product"
    params = {
        "key": KEEPA_API_KEY,
        "domain": domain_id,
        "asin": ",".join(asin_batch),
        "history": 0,
        "stats": 1,
        "rating": 1,
        "wait": 60,  # block up to 60s to throttle and avoid 429s
    }
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if "error" in data:
                raise Exception(data["error"]["message"])
            return data.get("products", [])
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                print(f"    ❌ Batch failed after {MAX_RETRIES} attempts: {exc}")
                return []
            print(f"    ⚠️ Batch failed (attempt {attempt + 1}): {exc}")
            exponential_backoff(attempt)
    return []


def stream_fetch_prices(
    api: keepa.Keepa,
    asin_data: Dict,
    fx_rates: Dict[str, float],
    checkpoint_mgr: CheckpointManager,
    writer: StreamingParquetWriter,
) -> Tuple[int, int]:
    all_batches = prepare_batch_list(asin_data)
    state = checkpoint_mgr.load_state()
    batch_offset = state.get("batch_offset", 0)
    if batch_offset:
        print(f"🔄 Resuming from batch {batch_offset}")
    total_api_calls = total_rows = 0
    batches_to_process = all_batches[batch_offset:]
    with tqdm(total=len(batches_to_process), desc="Processing") as pbar:
        for idx, (marketplace, category, asin_batch) in enumerate(batches_to_process):
            domain_key = f"Amazon{marketplace}"
            domain_id, currency = DOMAIN_MAPPING[domain_key]
            fx_rate = fx_rates[currency]
            products = fetch_batch_with_retry(api, asin_batch, domain_id)
            total_api_calls += 1
            rows = rows_from_products(products, marketplace, category, fx_rate) if products else []
            writer.write_batch(rows)
            total_rows += len(rows)
            checkpoint_mgr.save_state({"batch_offset": batch_offset + idx + 1})
            pbar.update(1)
            pbar.set_postfix(rows=total_rows, files=len(writer.uploaded_uris))
    return total_api_calls, total_rows


def load_to_bigquery(bq_client: bigquery.Client, gcs_uris: List[str]):
    if not gcs_uris:
        return 0
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND",
        clustering_fields=["asin", "marketplace"],
    )
    try:
        job = bq_client.load_table_from_uri(gcs_uris, table_id, job_config=job_config)
        job.result()
        print(f"📊 Loaded {len(gcs_uris)} parquet files → BigQuery")
        return job.output_rows
    except Exception as exc:
        print(f"❌ BigQuery load failed: {exc}")
        return 0


def ensure_gcs_bucket(gcs: storage.Client, bucket_name: str):
    try:
        gcs.bucket(bucket_name).reload()
    except NotFound:
        print(f"📦 Creating GCS bucket {bucket_name}")
        gcs.create_bucket(bucket_name)


# ---------------------------------------------------------------------------
# Public entry-point
# ---------------------------------------------------------------------------

def run_pipeline():  # renamed from main()
    print("🌊 Starting Streaming Keepa Pipeline (simplified)")
    if not KEEPA_API_KEY or KEEPA_API_KEY == "your-keepa-api-key":
        print("❌ KEEPA_API_KEY not set")
        return
    # Fetch ASINs from BigQuery lookup table
    asin_table = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{os.getenv('ASIN_TABLE_ID', 'asin_lookup')}"
    query = f"""
SELECT domain, category, ARRAY_AGG(asin) as asins
FROM `{asin_table}`
GROUP BY domain, category
"""
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    job = bq_client.query(query)
    results = job.result()
    asin_data: Dict[str, Dict[str, List[str]]] = {}
    for row in results:
        domain = row['domain']
        category = row['category']
        asins = row['asins'] or []
        asin_data.setdefault(domain, {})[category] = asins
    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    api = keepa.Keepa(KEEPA_API_KEY)
    # One-time token refresh to prime refillRate using blocking mode
    try:
        api._request("token", {"key": KEEPA_API_KEY}, wait=True)
    except Exception as e:
        print(f"⚠️ Keepa token refresh failed: {e}")
    ensure_gcs_bucket(gcs_client, GCS_BUCKET)
    today_prefix = f"price_data/{date.today().isoformat()}"
    checkpoint = CheckpointManager(gcs_client, GCS_BUCKET, STATE_BLOB)
    writer = StreamingParquetWriter(gcs_client, GCS_BUCKET, today_prefix)
    fx_rates = get_fx_rates()
    calls, rows = stream_fetch_prices(api, asin_data, fx_rates, checkpoint, writer)
    gcs_uris = writer.close()
    loaded = load_to_bigquery(bq_client, gcs_uris)
    if loaded:
        checkpoint.clear_state()
        print(f"🎉 Done. API calls: {calls}, rows: {rows}, loaded: {loaded}")
    else:
        print("⚠️ Completed but nothing loaded to BigQuery")


if __name__ == "__main__":
    run_pipeline() 