# app/services/jobs_service.py
from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

from app.job_platforms.unstop import OUTPUT_COLUMNS, unstop as unstop_platform
from app.ops.job_rtdb import (
    snapshot_prune_delete_and_save,  # Step-0 baseline sync + expiry purge
    upload_rows_push_keys,           # push keys => Firebase generates ids
)

# -------------------------------------------------------------------
# Output directories
# -------------------------------------------------------------------
OUTPUT_DIR_EXTRACT = Path("output") / "extract data"
OUTPUT_DIR_LATEST = Path("output") / "extracted_latest_jobs"

# -------------------------------------------------------------------
# Defaults / governance knobs
# -------------------------------------------------------------------
DEFAULT_SOURCES = ["unstop"]
DEFAULT_PER_PAGE = 18

# keep safe default because Unstop often breaks after page 1/2
DEFAULT_MAX_PAGES = 1

DEFAULT_OUT_PREFIX = "unstop_jobs"
FIREBASE_NODE_PATH = os.getenv("FIREBASE_JOBS_NODE_PATH", "ai/jobs")


def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


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
      - clear output/extracted_latest
      - download firebase node
      - delete expired from firebase (deadline < today IST)
      - save cleaned snapshot locally
      - build existing_job_id_set

    Step-1:
      - platform extractors run and RETURN ONLY DELTA rows (platform-level dedupe)

    Step-2:
      - save delta json/csv
      - upload delta to firebase using push keys
    """

    OUTPUT_DIR_EXTRACT.mkdir(parents=True, exist_ok=True)

    # ---------------------------------------------------------------
    # Step-0 baseline snapshot + expiry purge
    # ---------------------------------------------------------------
    baseline = snapshot_prune_delete_and_save(
        node_path=FIREBASE_NODE_PATH,
        out_dir=OUTPUT_DIR_LATEST,
    )
    existing_job_ids = baseline.existing_job_id_set

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
    all_delta_rows: List[dict] = []

    for src in DEFAULT_SOURCES:
        extractor = registry.get(src)
        if extractor is None:
            raise RuntimeError(f"Unknown source configured: {src}")

        delta_rows, stats = extractor(
            per_page=per_page,
            max_pages=max_pages,
            existing_job_ids=existing_job_ids,
            stop_when_page_all_seen=True,
        )

        per_source_stats[src] = stats
        per_source_delta_counts[src] = len(delta_rows)
        all_delta_rows.extend(delta_rows)

    # ---------------------------------------------------------------
    # Step-2 Save delta output
    # ---------------------------------------------------------------
    ts = _timestamp_str()
    json_path = OUTPUT_DIR_EXTRACT / f"{DEFAULT_OUT_PREFIX}_{ts}.json"
    csv_path = OUTPUT_DIR_EXTRACT / f"{DEFAULT_OUT_PREFIX}_{ts}.csv"

    _write_json(all_delta_rows, json_path)
    _write_csv(all_delta_rows, csv_path)

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
            "existing_job_ids_count": len(existing_job_ids),
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
            "json_file": str(json_path),
            "csv_file": str(csv_path),
        },

        "firebase": firebase_result,
    }
