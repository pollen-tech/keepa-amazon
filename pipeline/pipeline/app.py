"""FastAPI web service for triggering Amazon Keepa price pipeline"""

import asyncio
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks, HTTPException

from pipeline.streaming_daily_pipeline import run_pipeline

app = FastAPI(title="Amazon Keepa Price Pipeline")

# Global pipeline status
status = {
    "running": False,
    "last_run": None,
    "last_result": None,
    "error": None
}

@app.get("/")
def health_check():
    """Health check for Cloud Run"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/status")
def get_status():
    """Get pipeline status"""
    return status

async def run_pipeline_task():
    """Run pipeline in background"""
    global status
    try:
        status["running"] = True
        status["error"] = None
        
        # Run pipeline in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, run_pipeline)
        
        status["last_run"] = datetime.utcnow().isoformat()
        status["last_result"] = "success"
        status["running"] = False
        
    except Exception as e:
        status["last_run"] = datetime.utcnow().isoformat()
        status["last_result"] = "error"
        status["error"] = str(e)
        status["running"] = False
        raise

@app.post("/trigger")
def trigger_pipeline(background_tasks: BackgroundTasks):
    """Trigger the pipeline"""
    if status["running"]:
        raise HTTPException(409, "Pipeline already running")
    
    background_tasks.add_task(run_pipeline_task)
    return {"status": "started", "timestamp": datetime.utcnow().isoformat()} 