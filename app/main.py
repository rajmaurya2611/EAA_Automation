from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from app.api.v1.jobs import router as jobs_router
from app.api.v1.competitions import router as competitions_router

app = FastAPI(title="EAA Automation", version="1.0.0")
app.include_router(jobs_router, prefix="/api/v1", tags=["jobs"])
app.include_router(competitions_router, prefix="/api/v1", tags=["competitions"])
