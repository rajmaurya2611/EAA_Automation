from fastapi import APIRouter, HTTPException

from app.services.aktupdf_news_service import download_top_aktu_pdfs
from app.services.paddle_extraction_service import (
    extract_all_pdfs_with_paddle,
    extract_one_pdf_with_paddle,
    paddle_extraction_status,
)

router = APIRouter()


@router.post("/download-pdfs")
def download_aktu_pdfs():
    try:
        return download_top_aktu_pdfs()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/extract-paddle")
def extract_with_paddle():
    try:
        return extract_all_pdfs_with_paddle()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/extract-paddle/{file_name}")
def extract_one_with_paddle(file_name: str):
    try:
        return extract_one_pdf_with_paddle(file_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/extract-paddle/status")
def extract_paddle_status():
    try:
        return paddle_extraction_status()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))