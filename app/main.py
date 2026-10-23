from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from app.api.v1.jobs import router as jobs_router
from app.api.v1.competitions import router as competitions_router
from app.api.v1.aktunews_pdf import router as aktu_router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="EAA Automation", version="1.0.0")
app.include_router(jobs_router, prefix="/api/v1", tags=["jobs"])
app.include_router(competitions_router, prefix="/api/v1", tags=["competitions"])
app.include_router(aktu_router, prefix="/api/v1", tags=["aktu"])

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173","https://eaamanagement.vercel.app","http://localhost:5174"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
