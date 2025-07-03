"""Pytest configuration and shared fixtures for the Amazon Keepa pipeline service"""

import pytest


def pytest_addoption(parser):
    """Add custom command line options to control integration and Keepa tests"""
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run tests marked as integration (GCP access)"
    )
    parser.addoption(
        "--run-keepa",
        action="store_true",
        default=False,
        help="Run tests marked as keepa (Keepa API access)"
    )


def pytest_configure(config):
    """Register custom markers"""
    config.addinivalue_line(
        "markers",
        "integration: mark test as requiring GCP integration"
    )
    config.addinivalue_line(
        "markers",
        "keepa: mark test as requiring Keepa API access"
    )


def pytest_collection_modifyitems(config, items):
    """Skip marked tests unless corresponding flags are provided"""
    if not config.getoption("--run-integration"):
        skip_integration = pytest.mark.skip(
            reason="need --run-integration option to run"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
    
    if not config.getoption("--run-keepa"):
        skip_keepa = pytest.mark.skip(
            reason="need --run-keepa option to run"
        )
        for item in items:
            if "keepa" in item.keywords:
                item.add_marker(skip_keepa)


@pytest.fixture
def sample_asin_data():
    """Sample ASIN data for batch list tests"""
    return {
        "AmazonUS": {
            "Electronics": ["B123", "B456", "B789"],
            "Books": ["B111", "B222"]
        },
        "AmazonGB": {
            "Electronics": ["B333", "B444"]
        }
    }

@pytest.fixture
def sample_keepa_product():
    """Sample Keepa product data for rows_from_products tests"""
    return {
        "asin": "B123456789",
        "stats": {
            "current": [0, 0, 0, 1999],  # list price only
            "rating": [450, 4.5]  # [count, rating]
        }
    } 