# app/api/v1/jobs.py
from fastapi import APIRouter, HTTPException
import traceback

from app.services.jobs_service import job

router = APIRouter()

@router.get("/jobs")
def run_jobs():
    try:
        return job()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
