import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PDF_DIR = Path("downloads/aktunews_pdfs")
JSON_DIR = Path("downloads/aktu_json")
MARKDOWN_DIR = Path("downloads/aktu_markdown")
INDEX_FILE = JSON_DIR / "extraction_index.json"


def ensure_extraction_dirs() -> None:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    MARKDOWN_DIR.mkdir(parents=True, exist_ok=True)


def load_index() -> dict[str, Any]:
    if not INDEX_FILE.exists():
        return {}

    try:
        return json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_index(index_data: dict[str, Any]) -> None:
    INDEX_FILE.write_text(
        json.dumps(index_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def get_pdf_files() -> list[Path]:
    if not PDF_DIR.exists():
        return []
    return sorted([p for p in PDF_DIR.glob("*.pdf") if p.is_file()])


def get_output_paths(pdf_path: Path) -> tuple[Path, Path]:
    stem = pdf_path.stem
    json_path = JSON_DIR / f"{stem}.json"
    md_path = MARKDOWN_DIR / f"{stem}.md"
    return json_path, md_path


def get_file_mtime_iso(path: Path) -> str:
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def should_skip_extraction(pdf_path: Path, index_data: dict[str, Any]) -> bool:
    key = pdf_path.name
    record = index_data.get(key)

    if not record:
        return False

    current_mtime = get_file_mtime_iso(pdf_path)

    json_path, md_path = get_output_paths(pdf_path)

    return (
        record.get("source_mtime") == current_mtime
        and json_path.exists()
        and md_path.exists()
    )


def update_index_record(
    index_data: dict[str, Any],
    pdf_path: Path,
    json_path: Path,
    md_path: Path,
    status: str,
    error: str | None = None,
) -> None:
    index_data[pdf_path.name] = {
        "source_file": pdf_path.name,
        "source_path": str(pdf_path),
        "source_mtime": get_file_mtime_iso(pdf_path),
        "json_path": str(json_path),
        "markdown_path": str(md_path),
        "status": status,
        "error": error,
        "updated_at": utc_now_iso(),
    }