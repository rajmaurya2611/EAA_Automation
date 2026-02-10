# app/api/v1/competitions.py
from fastapi import APIRouter, HTTPException
import traceback

from app.services.competitions_service import competitions

router = APIRouter()

@router.get("/competitions")
def run_competitions():
    try:
        return competitions()
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
