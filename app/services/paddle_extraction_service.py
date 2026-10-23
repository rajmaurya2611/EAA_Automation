import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from paddleocr import PPStructureV3

PDF_DIR = Path("downloads/aktunews_pdfs")
JSON_DIR = Path("downloads/aktu_json")
INDEX_FILE = JSON_DIR / "paddle_extraction_index.json"
TEMP_OUTPUT_DIR = Path("downloads/_paddle_temp")

_PIPELINE = None


def _ensure_dirs() -> None:
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    TEMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def _load_index() -> dict[str, Any]:
    if not INDEX_FILE.exists():
        return {}
    try:
        return json.loads(INDEX_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _save_index(index_data: dict[str, Any]) -> None:
    INDEX_FILE.write_text(
        json.dumps(index_data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


def _output_json_path(pdf_path: Path) -> Path:
    return JSON_DIR / f"{pdf_path.stem}.json"


def _should_skip(pdf_path: Path, index_data: dict[str, Any]) -> bool:
    rec = index_data.get(pdf_path.name)
    out_json = _output_json_path(pdf_path)

    if not rec:
        return False

    return (
        rec.get("source_mtime") == _mtime_iso(pdf_path)
        and out_json.exists()
        and rec.get("status") == "success"
    )


def _update_index(
    index_data: dict[str, Any],
    pdf_path: Path,
    status: str,
    error: str | None = None,
    extracted_text_chars: int = 0,
) -> None:
    index_data[pdf_path.name] = {
        "source_file": pdf_path.name,
        "source_path": str(pdf_path),
        "source_mtime": _mtime_iso(pdf_path),
        "json_path": str(_output_json_path(pdf_path)),
        "status": status,
        "error": error,
        "extracted_text_chars": extracted_text_chars,
        "updated_at": _utc_now(),
    }


def _clear_temp_output() -> None:
    if TEMP_OUTPUT_DIR.exists():
        shutil.rmtree(TEMP_OUTPUT_DIR, ignore_errors=True)
    TEMP_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _find_generated_json() -> Path | None:
    files = sorted(
        TEMP_OUTPUT_DIR.rglob("*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def _build_pipeline() -> PPStructureV3:
    return PPStructureV3(
        lang="hi",
        ocr_version="PP-OCRv3",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
    )


def _get_pipeline() -> PPStructureV3:
    global _PIPELINE
    if _PIPELINE is None:
        _PIPELINE = _build_pipeline()
    return _PIPELINE


def _clean_line(text: str) -> str:
    text = (text or "").replace("\u200c", " ").replace("\u200d", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _is_useful_block_label(label: str) -> bool:
    ignore = {
        "header",
        "footer",
        "aside_text",
        "header_image",
        "footer_image",
        "image",
        "table",
        "chart",
        "formula",
        "seal",
    }
    return (label or "").strip().lower() not in ignore


def _sorted_parsing_blocks(data: dict[str, Any]) -> list[dict[str, Any]]:
    blocks = data.get("parsing_res_list", []) or []

    def sort_key(block: dict[str, Any]):
        order = block.get("block_order")
        if order is None:
            order = 10**9
        bbox = block.get("block_bbox") or [0, 0, 0, 0]
        y = bbox[1] if len(bbox) > 1 else 0
        x = bbox[0] if len(bbox) > 0 else 0
        return (order, y, x)

    return sorted(blocks, key=sort_key)


def _build_summary_ready_blocks(data: dict[str, Any]) -> list[dict[str, Any]]:
    blocks = _sorted_parsing_blocks(data)
    cleaned_blocks: list[dict[str, Any]] = []

    for block in blocks:
        label = (block.get("block_label") or "").strip().lower()
        content = _clean_line(block.get("block_content", ""))

        if not content:
            continue

        if not _is_useful_block_label(label):
            continue

        cleaned_blocks.append(
            {
                "label": label,
                "text": content,
                "order": block.get("block_order"),
                "bbox": block.get("block_bbox"),
            }
        )

    return cleaned_blocks


def _build_text_from_blocks(blocks: list[dict[str, Any]]) -> str:
    parts: list[str] = []

    for block in blocks:
        label = block["label"]
        text = block["text"]

        if label == "doc_title":
            parts.append(f"शीर्षक: {text}")
        else:
            parts.append(text)

    text = "\n".join(parts)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text


def _build_text_from_ocr_lines(data: dict[str, Any]) -> str:
    ocr = data.get("overall_ocr_res", {}) or {}
    rec_texts = ocr.get("rec_texts", []) or []
    cleaned = [_clean_line(x) for x in rec_texts if _clean_line(x)]
    return "\n".join(cleaned).strip()


def _detect_primary_title(blocks: list[dict[str, Any]]) -> str | None:
    for block in blocks:
        if block["label"] == "doc_title" and block["text"]:
            return block["text"]
    return None


def _extract_primary_date(data: dict[str, Any]) -> str | None:
    text_blob = _build_text_from_ocr_lines(data)
    patterns = [
        r"\d{1,2}[./-]\d{1,2}[./-]\d{2,4}",
        r"\d{1,2}\s*,?\s*[अ-हA-Za-z]+\s*,?\s*\d{4}",
        r"\d{4}",
    ]
    for pattern in patterns:
        m = re.search(pattern, text_blob)
        if m:
            return m.group(0)
    return None


def _extract_reference_number(data: dict[str, Any]) -> str | None:
    text_blob = _build_text_from_ocr_lines(data)
    lines = text_blob.splitlines()

    for line in lines[:8]:
        if any(token in line for token in ["पांक", "पत्रांक", "Ref", "No", "संख्या"]):
            return line.strip()

    return None


def _build_summary_ready_payload(raw_data: dict[str, Any]) -> dict[str, Any]:
    blocks = _build_summary_ready_blocks(raw_data)
    block_text = _build_text_from_blocks(blocks)
    ocr_text = _build_text_from_ocr_lines(raw_data)

    summary_ready_text = block_text if len(block_text) >= 60 else ocr_text

    metadata = {
        "title": _detect_primary_title(blocks),
        "date": _extract_primary_date(raw_data),
        "reference_number": _extract_reference_number(raw_data),
        "page_count": raw_data.get("page_count"),
        "source_file": Path(raw_data.get("input_path", "")).name if raw_data.get("input_path") else None,
    }

    return {
        "summary_ready_metadata": metadata,
        "summary_ready_blocks": blocks,
        "summary_ready_text": summary_ready_text.strip(),
        "summary_ready_text_chars": len(summary_ready_text.strip()),
    }


def _merge_final_output(raw_data: dict[str, Any]) -> dict[str, Any]:
    payload = _build_summary_ready_payload(raw_data)
    merged = {
        **raw_data,
        **payload,
        "post_processed_at": _utc_now(),
    }
    return merged


def extract_one_pdf_with_paddle(file_name: str) -> dict:
    _ensure_dirs()

    pdf_path = PDF_DIR / file_name
    if not pdf_path.exists() or pdf_path.suffix.lower() != ".pdf":
        return {
            "message": f"PDF not found: {file_name}",
            "processed_file": None,
        }

    index_data = _load_index()
    if _should_skip(pdf_path, index_data):
        return {
            "message": "File already extracted and unchanged.",
            "processed_file": index_data.get(pdf_path.name),
        }

    _update_index(index_data, pdf_path, status="processing")
    _save_index(index_data)

    try:
        pipeline = _get_pipeline()

        _clear_temp_output()

        output = pipeline.predict(str(pdf_path))
        for res in output:
            res.save_to_json(save_path=str(TEMP_OUTPUT_DIR))

        generated_json = _find_generated_json()
        if generated_json is None or not generated_json.exists():
            raise FileNotFoundError("PaddleOCR did not generate a JSON output file.")

        raw_data = json.loads(generated_json.read_text(encoding="utf-8"))
        final_data = _merge_final_output(raw_data)

        out_json = _output_json_path(pdf_path)
        out_json.write_text(
            json.dumps(final_data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        extracted_text_chars = len(final_data.get("summary_ready_text", "").strip())
        status = "success" if extracted_text_chars >= 20 else "no_text_extracted"
        error = None if status == "success" else "Pipeline completed, but meaningful text was not found in the summary-ready output."

        _update_index(
            index_data=index_data,
            pdf_path=pdf_path,
            status=status,
            error=error,
            extracted_text_chars=extracted_text_chars,
        )
        _save_index(index_data)

        return {
            "message": f"Processed file: {file_name}",
            "processed_file": {
                "source_file": pdf_path.name,
                "source_path": str(pdf_path),
                "json_path": str(out_json),
                "status": status,
                "error": error,
                "extracted_text_chars": extracted_text_chars,
            },
        }

    except Exception as exc:
        _update_index(index_data, pdf_path, status="failed", error=str(exc))
        _save_index(index_data)
        return {
            "message": f"Extraction failed for {file_name}",
            "processed_file": {
                "source_file": pdf_path.name,
                "status": "failed",
                "error": str(exc),
            },
        }


def extract_all_pdfs_with_paddle() -> dict:
    _ensure_dirs()

    pdf_files = sorted([p for p in PDF_DIR.glob("*.pdf") if p.is_file()])
    if not pdf_files:
        return {
            "message": "No PDFs found in downloads/aktunews_pdfs.",
            "processed_files": [],
            "skipped_files": [],
        }

    index_data = _load_index()
    processed_files = []
    skipped_files = []

    for pdf_path in pdf_files:
        if _should_skip(pdf_path, index_data):
            skipped_files.append(
                {
                    "source_file": pdf_path.name,
                    "reason": "Already extracted and unchanged",
                    "json_path": str(_output_json_path(pdf_path)),
                }
            )
            continue

        result = extract_one_pdf_with_paddle(pdf_path.name)
        processed_files.append(result["processed_file"])

    return {
        "message": f"Processed {len(processed_files)} PDFs. Skipped {len(skipped_files)} unchanged files.",
        "processed_files": processed_files,
        "skipped_files": skipped_files,
        "total_found": len(pdf_files),
    }


def paddle_extraction_status() -> dict:
    _ensure_dirs()
    return _load_index()