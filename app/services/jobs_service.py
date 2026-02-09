# app/services/jobs_service.py
from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from app.job_platforms.unstop import OUTPUT_COLUMNS, unstop as unstop_platform
from app.ops.job_rtdb import (
    snapshot_prune_delete_and_save,  # Step-0 baseline sync + expiry purge
    upload_rows_push_keys,           # push keys => Firebase generates ids
)

# -------------------------------------------------------------------
# Output directories
# -------------------------------------------------------------------
# Base: output/extract data/jobs/<platform>/
OUTPUT_DIR_EXTRACT_BASE = Path("output") / "extract data" / "jobs"
OUTPUT_DIR_LATEST = Path("output") / "extracted_latest" /"jobs"

# -------------------------------------------------------------------
# Defaults / governance knobs
# -------------------------------------------------------------------
DEFAULT_SOURCES = ["unstop"]
DEFAULT_PER_PAGE = 18

# keep safe default because Unstop often breaks after page 1/2
DEFAULT_MAX_PAGES = 1

FIREBASE_NODE_PATH = os.getenv("FIREBASE_JOBS_NODE_PATH", "ai/jobs")


def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _delete_all_files_in_dir(dir_path: Path) -> int:
    """
    Deletes all files in directory (not folders). Returns count deleted.
    """
    if not dir_path.exists():
        return 0

    deleted = 0
    for p in dir_path.iterdir():
        if p.is_file():
            try:
                p.unlink()
                deleted += 1
            except Exception:
                # best-effort: don't fail pipeline for one locked file
                pass
    return deleted


def _write_json(rows: List[dict], path: Path) -> None:
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_csv(rows: List[dict], path: Path) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in OUTPUT_COLUMNS})


def job() -> Dict[str, object]:
    """
    Orchestrator:

    Step-0:
      - clear output/extracted_latest_jobs
      - download firebase node
      - delete expired from firebase (deadline < today IST)
      - save cleaned snapshot locally
      - build existing composite-key set (platform:oppurtunity_type:job_id)

    Step-1:
      - each platform extractor returns DELTA rows (deduped against baseline set)

    Step-2:
      - per-platform file output:
          output/extract data/jobs/<platform>/
        * purge old files in that folder first
        * write fresh JSON + CSV

    Step-3:
      - upload ALL delta rows to firebase (push keys)
    """

    OUTPUT_DIR_EXTRACT_BASE.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------
    # Step-0 baseline snapshot + expiry purge
    # ---------------------------------------------------------------
    baseline = snapshot_prune_delete_and_save(
        node_path=FIREBASE_NODE_PATH,
        out_dir=OUTPUT_DIR_LATEST,
    )
    existing_keys = baseline.existing_job_id_set  # Set[str] composite keys

    # ---------------------------------------------------------------
    # Registry pattern: platform returns (delta_rows, platform_stats)
    # ---------------------------------------------------------------
    registry = {
        "unstop": unstop_platform,
        # Future:
        # "internshala": internshala_platform,
        # "linkedin": linkedin_platform,
    }

    per_page = DEFAULT_PER_PAGE
    max_pages = DEFAULT_MAX_PAGES

    per_source_stats: Dict[str, dict] = {}
    per_source_delta_counts: Dict[str, int] = {}
    per_source_files: Dict[str, Dict[str, str]] = {}
    per_source_deleted_old_files: Dict[str, int] = {}

    all_delta_rows: List[dict] = []
    ts = _timestamp_str()

    for src in DEFAULT_SOURCES:
        extractor = registry.get(src)
        if extractor is None:
            raise RuntimeError(f"Unknown source configured: {src}")

        delta_rows, stats = extractor(
            per_page=per_page,
            max_pages=max_pages,
            existing_job_ids=existing_keys,      # ✅ composite keys
            stop_when_page_all_seen=True,
        )

        per_source_stats[src] = stats
        per_source_delta_counts[src] = len(delta_rows)
        all_delta_rows.extend(delta_rows)

        # -----------------------------------------------------------
        # Step-2 (per platform): purge old + write fresh files
        # output/extract data/jobs/<platform>/
        # -----------------------------------------------------------
        platform_dir = OUTPUT_DIR_EXTRACT_BASE / src
        platform_dir.mkdir(parents=True, exist_ok=True)

        deleted_old = _delete_all_files_in_dir(platform_dir)
        per_source_deleted_old_files[src] = deleted_old

        json_path = platform_dir / f"{src}_jobs_{ts}.json"
        csv_path = platform_dir / f"{src}_jobs_{ts}.csv"

        _write_json(delta_rows, json_path)
        _write_csv(delta_rows, csv_path)

        per_source_files[src] = {
            "json_file": str(json_path),
            "csv_file": str(csv_path),
        }

    # ---------------------------------------------------------------
    # Step-3 Upload delta only (firebase generates keys)
    # ---------------------------------------------------------------
    firebase_result = upload_rows_push_keys(
        node_path=FIREBASE_NODE_PATH,
        rows=all_delta_rows,
    )

    return {
        "ok": True,

        "baseline": {
            "node_path": baseline.node_path,
            "today_ist": baseline.today_ist,
            "deleted_local_files": baseline.deleted_local_files,
            "expired_keys_count": baseline.expired_keys_count,
            "expired_deleted_from_firebase": baseline.expired_deleted_from_firebase,
            "kept_count_after_prune": baseline.kept_count,
            "saved_file": baseline.saved_file,
            "existing_job_ids_count": len(existing_keys),
        },

        "scrape": {
            "sources": DEFAULT_SOURCES,
            "per_page": per_page,
            "max_pages": max_pages,
            "delta_total": len(all_delta_rows),
            "delta_by_source": per_source_delta_counts,
            "platform_stats": per_source_stats,
        },

        "files": {
            "base_dir": str(OUTPUT_DIR_EXTRACT_BASE),
            "deleted_old_files_by_source": per_source_deleted_old_files,
            "by_source": per_source_files,
        },

        "firebase": firebase_result,
    }
