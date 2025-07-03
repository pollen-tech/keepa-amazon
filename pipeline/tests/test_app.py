"""Unit tests for FastAPI app"""

import pytest
from unittest.mock import Mock, patch, AsyncMock
from fastapi.testclient import TestClient

@pytest.fixture
def client():
    """Create test client for FastAPI app"""
    from pipeline.app import app
    return TestClient(app)

def test_health_check(client):
    """Test health check endpoint"""
    response = client.get("/")
    assert response.status_code == 200
    
    data = response.json()
    assert data["status"] == "healthy"
    assert "timestamp" in data

def test_status_endpoint(client):
    """Test status endpoint"""
    response = client.get("/status")
    assert response.status_code == 200
    
    data = response.json()
    assert "running" in data
    assert "last_run" in data
    assert "last_result" in data
    assert "error" in data

def test_trigger_endpoint_success(client):
    """Test trigger endpoint when pipeline is not running"""
    with patch('pipeline.app.status', {"running": False}):
        response = client.post("/trigger")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "started"
        assert "timestamp" in data

def test_trigger_endpoint_already_running(client):
    """Test trigger endpoint when pipeline is already running"""
    with patch('pipeline.app.status', {"running": True}):
        response = client.post("/trigger")
        assert response.status_code == 409

@pytest.mark.asyncio
async def test_run_pipeline_task_success():
    """Test run_pipeline_task with successful execution"""
    from pipeline.app import run_pipeline_task, status
    
    with patch('pipeline.app.run_pipeline') as mock_run_pipeline:
        mock_run_pipeline.return_value = None
        
        # Reset status
        status.update({
            "running": False,
            "last_run": None,
            "last_result": None,
            "error": None
        })
        
        await run_pipeline_task()
        
        assert status["running"] == False
        assert status["last_result"] == "success"
        assert status["error"] is None
        assert status["last_run"] is not None

@pytest.mark.asyncio
async def test_run_pipeline_task_error():
    """Test run_pipeline_task with pipeline error"""
    from pipeline.app import run_pipeline_task, status
    
    with patch('pipeline.app.run_pipeline') as mock_run_pipeline:
        mock_run_pipeline.side_effect = Exception("Pipeline error")
        
        # Reset status
        status.update({
            "running": False,
            "last_run": None,
            "last_result": None,
            "error": None
        })
        
        with pytest.raises(Exception, match="Pipeline error"):
            await run_pipeline_task()
        
        assert status["running"] == False
        assert status["last_result"] == "error"
        assert status["error"] == "Pipeline error"
        assert status["last_run"] is not None 