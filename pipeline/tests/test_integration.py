#!/usr/bin/env python3
"""
Integration test - verify all components work before deployment
"""

import json
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch

def test_imports():
    """Test all imports are available"""
    from google.cloud import bigquery, storage, secretmanager
    import keepa
    import pandas as pd
    import pyarrow as pa
    from fastapi import FastAPI
    assert True  # If we get here, imports work

def test_configuration():
    """Test configuration loading"""
    from pipeline.config import (
        GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_ID, 
        KEEPA_API_KEY, TARGET_COLUMNS
    )
    assert GCP_PROJECT_ID == "pollen-sandbox-warehouse"
    assert GCP_DATASET_ID == "amazon_keepa_products"  
    assert GCP_TABLE_ID == "price_history"
    assert isinstance(TARGET_COLUMNS, list)
    assert len(TARGET_COLUMNS) > 0

@pytest.mark.integration
def test_bigquery_connection():
    """Test BigQuery connection and table exists"""
    from pipeline.config import GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_ID
    from google.cloud import bigquery
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    # Check dataset exists
    dataset_ref = client.dataset(GCP_DATASET_ID)
    dataset = client.get_dataset(dataset_ref)
    assert dataset.dataset_id == GCP_DATASET_ID
    
    # Check table exists
    table_ref = dataset_ref.table(GCP_TABLE_ID)
    table = client.get_table(table_ref)
    assert table.table_id == GCP_TABLE_ID

@pytest.mark.integration 
def test_gcs_access():
    """Test GCS bucket access"""
    from pipeline.config import GCP_PROJECT_ID
    from google.cloud import storage
    
    gcs_client = storage.Client(project=GCP_PROJECT_ID)
    bucket_name = f"{GCP_PROJECT_ID}-keepa-staging"
    
    # Just test we can create a bucket client (bucket may not exist yet)
    bucket = gcs_client.bucket(bucket_name)
    assert bucket.name == bucket_name

@pytest.mark.keepa
def test_keepa_api():
    """Test Keepa API connection"""
    from pipeline.config import KEEPA_API_KEY
    import keepa
    
    if not KEEPA_API_KEY:
        pytest.skip("KEEPA_API_KEY not available")
    
    api = keepa.Keepa(KEEPA_API_KEY)
    tokens = api.tokens_left
    assert tokens >= 0  # Could be 0 if rate limited

def test_asin_data_file():
    """Test ASIN data file is accessible"""
    asin_path = Path('../asins_fetch_via_scraping/asin_output_scraping/all_domains_top_asins.json')
    
    if not asin_path.exists():
        pytest.skip(f"ASIN file not found: {asin_path}")
    
    with open(asin_path) as f:
        asin_data = json.load(f)
    
    assert isinstance(asin_data, dict)
    total_asins = sum(
        len(asins) for domain, categories in asin_data.items()
        for asins in categories.values()
    )
    assert total_asins > 0

def test_streaming_pipeline_import():
    """Test streaming pipeline components can be imported"""
    from pipeline.streaming_daily_pipeline import (
        StreamingParquetWriter, CheckpointManager,
        prepare_batch_list, rows_from_products
    )
    assert StreamingParquetWriter is not None
    assert CheckpointManager is not None
    assert prepare_batch_list is not None
    assert rows_from_products is not None

def test_fastapi_app():
    """Test FastAPI app can be created"""
    from pipeline.app import app
    assert app is not None
    assert hasattr(app, 'routes')
    
    # Check expected routes exist
    route_paths = [route.path for route in app.routes]
    assert "/" in route_paths
    assert "/status" in route_paths
    assert "/trigger" in route_paths 