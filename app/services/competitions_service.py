# app/services/competitions_service.py
from __future__ import annotations

import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from app.competition_platforms.unstop import OUTPUT_COLUMNS, unstop_competitions
from app.ops.competition_rtdb import snapshot_prune_delete_and_save, upload_rows_push_keys

OUTPUT_DIR_EXTRACT = Path("output") / "extract data" / "competitions"
OUTPUT_DIR_LATEST = Path("output") / "extracted_latest" / "competitions"

DEFAULT_SOURCES = ["unstop"]
DEFAULT_PER_PAGE = 18
DEFAULT_MAX_PAGES = 1

DEFAULT_OUT_PREFIX = "unstop_competitions"
FIREBASE_NODE_PATH = os.getenv("FIREBASE_COMPETITIONS_NODE_PATH", "ai/competitions")


def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _clear_dir_files(dir_path: Path) -> int:
    if not dir_path.exists():
        return 0
    deleted = 0
    for p in dir_path.iterdir():
        if p.is_file():
            try:
                p.unlink()
                deleted += 1
            except Exception:
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


def competitions() -> Dict[str, object]:
    OUTPUT_DIR_EXTRACT.mkdir(parents=True, exist_ok=True)

    baseline = snapshot_prune_delete_and_save(
        node_path=FIREBASE_NODE_PATH,
        out_dir=OUTPUT_DIR_LATEST,
    )
    existing_comp_keys = baseline.existing_comp_key_set

    registry = {
        "unstop": unstop_competitions,
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
            existing_comp_keys=existing_comp_keys,
            stop_when_page_all_seen=True,
        )

        per_source_stats[src] = stats
        per_source_delta_counts[src] = len(delta_rows)
        all_delta_rows.extend(delta_rows)

        platform_dir = OUTPUT_DIR_EXTRACT / src
        platform_dir.mkdir(parents=True, exist_ok=True)
        _clear_dir_files(platform_dir)

        ts = _timestamp_str()
        json_path = platform_dir / f"{DEFAULT_OUT_PREFIX}_{ts}.json"
        csv_path = platform_dir / f"{DEFAULT_OUT_PREFIX}_{ts}.csv"

        _write_json(delta_rows, json_path)
        _write_csv(delta_rows, csv_path)

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
            "existing_comp_keys_count": len(existing_comp_keys),
        },
        "scrape": {
            "sources": DEFAULT_SOURCES,
            "per_page": per_page,
            "max_pages": max_pages,
            "delta_total": len(all_delta_rows),
            "delta_by_source": per_source_delta_counts,
            "platform_stats": per_source_stats,
        },
        "firebase": firebase_result,
    }
