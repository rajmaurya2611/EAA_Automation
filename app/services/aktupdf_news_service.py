import json
from pathlib import Path

from app.utils.aktupdf_news_utils import (
    download_file,
    extract_pdf_links_from_circular_page,
    fetch_html,
    sanitize_filename,
    url_to_hash,
)

# IMPORTANT:
# public page is only wrapper; scrape the internal circular listing source
AKTU_SOURCE_URL = "https://erp.aktu.ac.in/WebPages/Public/Circular/CircularForWebsite.aspx"

DOWNLOAD_DIR = Path("downloads/aktunews_pdfs")
METADATA_FILE = DOWNLOAD_DIR / "downloaded_files.json"
MAX_PDFS = 10


def load_downloaded_metadata() -> dict:
    if not METADATA_FILE.exists():
        return {}
    try:
        with open(METADATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_downloaded_metadata(metadata: dict) -> None:
    with open(METADATA_FILE, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)


def is_duplicate(pdf_url: str, metadata: dict) -> bool:
    return url_to_hash(pdf_url) in metadata


def build_file_name(index: int, title: str) -> str:
    safe_title = sanitize_filename(title)
    return f"{index:02d}_{safe_title}.pdf"


def download_top_aktu_pdfs():
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    metadata = load_downloaded_metadata()
    html = fetch_html(AKTU_SOURCE_URL)
    pdf_links = extract_pdf_links_from_circular_page(html, AKTU_SOURCE_URL)

    if not pdf_links:
        return {
            "message": "No PDF links found from AKTU source page.",
            "downloaded_files": [],
            "skipped_files": [],
            "source_url": AKTU_SOURCE_URL,
        }

    top_pdfs = pdf_links[:MAX_PDFS]

    downloaded_files = []
    skipped_files = []

    for index, pdf in enumerate(top_pdfs, start=1):
        pdf_url = pdf["url"]
        pdf_title = pdf["title"]

        if is_duplicate(pdf_url, metadata):
            skipped_files.append({
                "title": pdf_title,
                "url": pdf_url,
                "reason": "Already downloaded",
            })
            continue

        file_name = build_file_name(index, pdf_title)
        file_path = DOWNLOAD_DIR / file_name

        download_file(pdf_url, file_path)

        pdf_hash = url_to_hash(pdf_url)
        metadata[pdf_hash] = {
            "title": pdf_title,
            "url": pdf_url,
            "saved_as": file_name,
            "local_path": str(file_path),
        }

        downloaded_files.append({
            "title": pdf_title,
            "url": pdf_url,
            "saved_as": file_name,
            "local_path": str(file_path),
        })

    save_downloaded_metadata(metadata)

    return {
        "message": f"Downloaded {len(downloaded_files)} new PDFs. Skipped {len(skipped_files)} duplicates.",
        "source_url": AKTU_SOURCE_URL,
        "total_found_on_source_page": len(pdf_links),
        "processed_top_n": len(top_pdfs),
        "downloaded_files": downloaded_files,
        "skipped_files": skipped_files,
    }