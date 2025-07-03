"""Configuration for Amazon Keepa price pipeline"""

import os
from google.cloud import secretmanager

# define get_secret before usage
def get_secret(secret_id: str, project_id: str) -> str:
    """Get secret from Google Secret Manager"""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# GCP Configuration
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "pollen-sandbox-warehouse")
GCP_DATASET_ID = os.getenv("GCP_DATASET_ID", "amazon_keepa_products") 
GCP_TABLE_ID = os.getenv("GCP_TABLE_ID", "price_history")

# Secret Manager ID for Keepa API key
SECRET_NAME = os.getenv("KEEPA_API_SECRET_NAME", "keepa-api-key")
try:
    KEEPA_API_KEY = get_secret(SECRET_NAME, GCP_PROJECT_ID).strip()
except Exception as e:
    # fallback to environment variable if secret fetch fails
    KEEPA_API_KEY = os.getenv("KEEPA_API_KEY", "").strip()
    if not KEEPA_API_KEY:
        raise RuntimeError(f"Failed to fetch secret {SECRET_NAME}: {e}")

# BigQuery Schema Columns
TARGET_COLUMNS = [
    "date", "retail_price", "discounted_price", "rating",
    "asin", "marketplace", "category", "created_at", "ingestion_date"
] 