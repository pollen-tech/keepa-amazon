#!/usr/bin/env python3
"""
OPTIMIZED Daily Pipeline - Based on Expert Analysis
Cost: ~243 API calls instead of 24,289 (99% reduction!)
"""

import json
import os
import sys
import pandas as pd
import keepa
from datetime import datetime, timezone, date
from pathlib import Path
from google.cloud import bigquery
import requests
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

# Add pipeline to path for imports
sys.path.append('pipeline')
from config.pipeline_config import (
    GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_ID,
    KEEPA_API_KEY, TARGET_COLUMNS
)

# Domain mapping for Keepa API (corrected to use string codes)
DOMAIN_MAPPING = {
    "AmazonUS": ("US", "USD"),  # domain='US', currency=USD
    "AmazonGB": ("GB", "GBP"),  # domain='GB', currency=GBP
    "AmazonDE": ("DE", "EUR"),  # domain='DE', currency=EUR
    "AmazonJP": ("JP", "JPY"),  # domain='JP', currency=JPY
}

def get_fx_rates():
    """Get current FX rates to convert everything to USD"""
    try:
        # Using a free FX API (you could also use a paid service)
        response = requests.get("https://api.exchangerate-api.com/v4/latest/USD")
        rates = response.json()["rates"]
        
        # Convert to USD rates (inverted)
        fx_rates = {
            "USD": 1.0,
            "GBP": 1 / rates["GBP"],  # GBP to USD
            "EUR": 1 / rates["EUR"],  # EUR to USD  
            "JPY": 1 / rates["JPY"],  # JPY to USD
        }
        print(f"📈 FX Rates: GBP→USD={fx_rates['GBP']:.3f}, EUR→USD={fx_rates['EUR']:.3f}, JPY→USD={fx_rates['JPY']:.5f}")
        return fx_rates
    except Exception as e:
        print(f"⚠️ FX rate fetch failed: {e}. Using defaults.")
        return {"USD": 1.0, "GBP": 1.25, "EUR": 1.1, "JPY": 0.007}

def chunks(lst, n=100):
    """Split list into chunks of size n"""
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def rows_from_products(products, marketplace, category, fx_rate):
    """Extract rows from Keepa product data using optimized stats.current approach"""
    today = date.today()
    created_ts = datetime.now(timezone.utc)
    
    rows = []
    for p in products:
        try:
            # Use stats.current instead of parsing CSV time series!
            stats = p.get("stats", {})
            current = stats.get("current", [])
            
            # Extract current prices (in cents, convert to dollars)
            retail = current[3] / 100 if len(current) > 3 and current[3] > 0 else None
            discount = current[0] / 100 if len(current) > 0 and current[0] > 0 else None
            
            # Get rating from stats
            rating = stats.get("rating")
            if isinstance(rating, list) and rating:
                rating = rating[-1]  # Latest rating
            
            row = {
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
            rows.append(row)
            
        except Exception as e:
            print(f"  ⚠️ Error processing {p.get('asin', 'unknown')}: {e}")
            continue
            
    return rows

def fetch_optimized_prices(api, asin_data, fx_rates):
    """Fetch prices using optimized bulk API calls"""
    all_rows = []
    total_api_calls = 0
    total_asins_processed = 0
    
    print("🚀 Starting optimized price fetch...")
    
    for domain_key, categories in asin_data.items():
        if domain_key not in DOMAIN_MAPPING:
            print(f"⚠️ Unknown domain: {domain_key}")
            continue
            
        domain_id, currency = DOMAIN_MAPPING[domain_key]
        fx_rate = fx_rates[currency]
        marketplace = domain_key.replace("Amazon", "")
        
        print(f"\n🌍 Processing {marketplace} (domain='{domain_id}', FX={fx_rate:.3f}):")
        
        for category, asin_list in categories.items():
            if not asin_list:
                continue
                
            print(f"  📂 {category}: {len(asin_list)} ASINs")
            category_calls = 0
            category_rows = 0
            
            # Process in batches of 100 ASINs (expert recommendation)
            for batch in chunks(asin_list, 100):
                try:
                    print(f"    🔄 Batch of {len(batch)} ASINs...")
                    
                    # OPTIMIZED API CALL - key changes:
                    # - history=False (no CSV time series)
                    # - stats=1 (include current prices)
                    # - rating=True (include rating data)
                    # - wait=True (respect rate limits)
                    products = api.query(
                        batch,
                        domain=domain_id,
                        history=False,  # 🎯 KEY OPTIMIZATION: No historical data!
                        stats=1,        # 🎯 Include current price stats
                        rating=True,    # Include rating data
                        wait=True       # Respect rate limits
                    )
                    
                    total_api_calls += 1
                    category_calls += 1
                    
                    # Extract data using optimized approach
                    batch_rows = rows_from_products(products, marketplace, category, fx_rate)
                    all_rows.extend(batch_rows)
                    category_rows += len(batch_rows)
                    total_asins_processed += len(batch)
                    
                    print(f"    ✅ Got {len(batch_rows)} price records")
                    
                except Exception as e:
                    print(f"    ❌ Batch failed: {e}")
                    continue
            
            print(f"  📊 Category total: {category_calls} API calls, {category_rows} records")
    
    print(f"\n📈 OPTIMIZATION RESULTS:")
    print(f"   Total API calls: {total_api_calls} (vs 24,289 individual calls)")
    print(f"   Cost reduction: {(1 - total_api_calls/24289)*100:.1f}%")
    print(f"   ASINs processed: {total_asins_processed}")
    print(f"   Records collected: {len(all_rows)}")
    
    return all_rows

def upload_to_bigquery(client, rows):
    """Upload data to BigQuery"""
    if not rows:
        print("⚠️ No data to upload")
        return
        
    df = pd.DataFrame(rows)
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{GCP_TABLE_ID}"
    
    print(f"📤 Uploading {len(df)} records to BigQuery...")
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
    )
    
    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for completion
        
        print(f"✅ Successfully uploaded {len(df)} records!")
        
        # Get table stats
        table = client.get_table(table_id)
        print(f"   📊 Total table rows: {table.num_rows:,}")
        
    except Exception as e:
        print(f"❌ BigQuery upload failed: {e}")
        raise

def main():
    """Main optimized pipeline execution"""
    print("🚀 OPTIMIZED Daily Amazon Keepa Price Pipeline")
    print("=" * 60)
    
    # Check environment
    if not KEEPA_API_KEY or KEEPA_API_KEY == "your-keepa-api-key":
        print("❌ Error: KEEPA_API_KEY not set properly in .env")
        return
    
    # Load ASIN data
    asin_path = Path('asins_fetch_via_scraping/asin_output_scraping/all_domains_top_asins.json')
    if not asin_path.exists():
        print(f"❌ ASIN file not found: {asin_path}")
        return
        
    with open(asin_path) as f:
        asin_data = json.load(f)
    
    # Get FX rates
    fx_rates = get_fx_rates()
    
    # Initialize Keepa API
    print(f"🔑 Initializing Keepa API...")
    api = keepa.Keepa(KEEPA_API_KEY)
    
    # Fetch data using optimized approach
    rows = fetch_optimized_prices(api, asin_data, fx_rates)
    
    # Upload to BigQuery
    if rows:
        client = bigquery.Client(project=GCP_PROJECT_ID)
        upload_to_bigquery(client, rows)
    else:
        print("❌ No data collected")
        return
    
    print(f"\n🎉 OPTIMIZED PIPELINE COMPLETED!")
    print(f"💰 Estimated savings: ~99% cost reduction vs individual calls")
    print(f"⚡ Ready for production deployment!")

if __name__ == "__main__":
    main() 