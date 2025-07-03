"""Unit tests for streaming_daily_pipeline module"""

import pytest
from pipeline.streaming_daily_pipeline import prepare_batch_list, rows_from_products
from datetime import date

def test_prepare_batch_list(sample_asin_data):
    """prepare_batch_list should return correct batches"""
    batches = prepare_batch_list(sample_asin_data)
    assert isinstance(batches, list)
    # Each batch is a tuple of marketplace, category, list of ASINs
    for marketplace, category, asin_list in batches:
        assert isinstance(marketplace, str)
        assert isinstance(category, str)
        assert isinstance(asin_list, list)
    # Check total number of ASINs matches input
    total_input = sum(
        len(asins) for domain in sample_asin_data.values() for asins in domain.values()
    )
    total_batch = sum(len(batch[2]) for batch in batches)
    assert total_input == total_batch

def test_rows_from_products(sample_keepa_product):
    """rows_from_products should transform Keepa data to rows"""
    products = [sample_keepa_product]
    rows = rows_from_products(products, marketplace="US", category="Electronics", fx_rate=1.0)
    assert isinstance(rows, list)
    assert len(rows) == 1
    row = rows[0]
    assert row["asin"] == sample_keepa_product["asin"]
    assert row["marketplace"] == "US"
    assert row["category"] == "Electronics"
    assert isinstance(row["date"], date)
    # Price fields are in dollars
    assert "retail_price" in row and row["retail_price"] is not None
    assert "discounted_price" in row
    assert "rating" in row 

def test_stream_fetch_prices_checkpointing(monkeypatch, sample_asin_data, sample_keepa_product):
    """stream_fetch_prices should save state after each batch"""
    from pipeline.streaming_daily_pipeline import stream_fetch_prices

    # Prepare fake batch list
    batches = [
        ("US", "Electronics", ["A"]),
        ("GB", "Books", ["B"]),
        ("JP", "Music", ["C"]),
    ]
    monkeypatch.setattr(
        "pipeline.streaming_daily_pipeline.prepare_batch_list",
        lambda x: batches
    )

    # Dummy API returns one product per batch
    class DummyApi:
        def query(self, asin_batch, domain, history, stats, rating, wait):
            return [sample_keepa_product]
    api = DummyApi()

    # FX rates mapping for all currencies
    fx_rates = {"USD": 1.0, "GBP": 1.0, "JPY": 1.0}

    # Track saved batch offsets
    saved_offsets = []
    class DummyCheckpoint:
        def load_state(self):
            return {"batch_offset": 0}
        def save_state(self, state):
            saved_offsets.append(state["batch_offset"])
        def clear_state(self):
            pass

    # Dummy writer that collects rows
    written_rows = []
    class DummyWriter:
        def __init__(self):
            self.uploaded_uris = []
        def write_batch(self, rows):
            written_rows.append(rows)
        def close(self):
            return []

    checkpoint = DummyCheckpoint()
    writer = DummyWriter()
    calls, total_rows = stream_fetch_prices(api, sample_asin_data, fx_rates, checkpoint, writer)

    # Should have processed all batches
    assert calls == len(batches)
    assert total_rows == len(batches)
    # Offsets saved after each batch: 1, 2, 3
    assert saved_offsets == [1, 2, 3]
    # Each write_batch should have been called
    assert len(written_rows) == len(batches)


def test_stream_fetch_prices_resume(monkeypatch, sample_asin_data, sample_keepa_product):
    """stream_fetch_prices should resume from the saved batch_offset"""
    from pipeline.streaming_daily_pipeline import stream_fetch_prices

    batches = [
        ("US", "Electronics", ["A"]),
        ("GB", "Books", ["B"]),
        ("JP", "Music", ["C"]),
    ]
    monkeypatch.setattr(
        "pipeline.streaming_daily_pipeline.prepare_batch_list",
        lambda x: batches
    )

    class DummyApi:
        def query(self, asin_batch, domain, history, stats, rating, wait):
            return [sample_keepa_product]
    api = DummyApi()

    fx_rates = {"USD": 1.0, "GBP": 1.0, "JPY": 1.0}

    saved_offsets = []
    class DummyCheckpoint:
        def __init__(self, offset):
            self.offset = offset
        def load_state(self):
            return {"batch_offset": self.offset}
        def save_state(self, state):
            saved_offsets.append(state["batch_offset"])
        def clear_state(self):
            pass

    class DummyWriter:
        def __init__(self):
            self.uploaded_uris = []
        def write_batch(self, rows):
            pass
        def close(self):
            return []

    # Start from offset=2, so only the 3rd batch runs
    checkpoint = DummyCheckpoint(offset=2)
    writer = DummyWriter()
    calls, total_rows = stream_fetch_prices(api, sample_asin_data, fx_rates, checkpoint, writer)

    assert calls == 1
    assert total_rows == 1
    # Only one save_state call: offset 3
    assert saved_offsets == [3] 