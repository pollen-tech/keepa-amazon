"""Configuration for Amazon Keepa price pipeline"""

import os
from google.cloud import secretmanager

def get_secret(secret_id: str, project_id: str) -> str:
    """Get secret from Google Secret Manager"""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception:
        return os.getenv(secret_id, "")

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "pollen-sandbox-warehouse")
GCP_DATASET_ID = os.getenv("GCP_DATASET_ID", "amazon_keepa_products") 
GCP_TABLE_ID = os.getenv("GCP_TABLE_ID", "price_history")

# Keepa API Key
KEEPA_API_KEY = get_secret("KEEPA_API_KEY", GCP_PROJECT_ID) or os.getenv("KEEPA_API_KEY")

# BigQuery Schema Columns
TARGET_COLUMNS = [
    "date", "retail_price", "discounted_price", "rating",
    "asin", "marketplace", "category", "created_at", "ingestion_date"
] 