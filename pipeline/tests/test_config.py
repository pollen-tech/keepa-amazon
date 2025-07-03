"""Unit tests for config module"""

import pytest
from unittest.mock import patch, Mock

def test_config_constants():
    """Test configuration constants are set correctly"""
    from pipeline.config import (
        GCP_PROJECT_ID, GCP_DATASET_ID, GCP_TABLE_ID, TARGET_COLUMNS
    )
    
    assert GCP_PROJECT_ID == "pollen-sandbox-warehouse"
    assert GCP_DATASET_ID == "amazon_keepa_products"
    assert GCP_TABLE_ID == "price_history"
    assert TARGET_COLUMNS == [
        "date", "retail_price", "discounted_price", "rating",
        "asin", "marketplace", "category", "created_at", "ingestion_date"
    ]

@patch('pipeline.config.secretmanager.SecretManagerServiceClient')
def test_get_secret_success(mock_client):
    """Test get_secret function with successful API call"""
    from pipeline.config import get_secret
    
    # Mock the secret manager response
    mock_instance = Mock()
    mock_client.return_value = mock_instance
    
    mock_response = Mock()
    mock_response.payload.data.decode.return_value = "test-secret-value"
    mock_instance.access_secret_version.return_value = mock_response
    
    result = get_secret("test-secret", "test-project")
    
    assert result == "test-secret-value"
    mock_instance.access_secret_version.assert_called_once()

@patch('pipeline.config.secretmanager.SecretManagerServiceClient')
@patch('pipeline.config.os.getenv')
def test_get_secret_fallback(mock_getenv, mock_client):
    """Test get_secret function falls back to environment variable"""
    from pipeline.config import get_secret
    
    # Mock secret manager to raise an exception
    mock_client.side_effect = Exception("Secret Manager error")
    mock_getenv.return_value = "env-secret-value"
    
    result = get_secret("test-secret", "test-project")
    
    assert result == "env-secret-value"
    mock_getenv.assert_called_once_with("test-secret", "")

def test_keepa_api_key_loading(monkeypatch):
    """Test KEEPA_API_KEY is loaded correctly via env var fallback"""
    # Set environment variable for Keepa API key
    monkeypatch.setenv("KEEPA_API_KEY", "test-api-key")
    # Reload config module to pick up the new env var
    import importlib
    import pipeline.config
    importlib.reload(pipeline.config)
    assert pipeline.config.KEEPA_API_KEY == "test-api-key" 