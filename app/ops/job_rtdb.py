# app/ops/job_rtdb.py
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import firebase_admin
from firebase_admin import credentials, db

# -------------------------------------------------------------------
# Singleton Firebase init
# -------------------------------------------------------------------
_firebase_app: Optional[firebase_admin.App] = None


def _init_firebase() -> firebase_admin.App:
    """
    Initialize Firebase Admin SDK ONCE per process.

    ENV required:
      - FIREBASE_DATABASE_URL
      - FIREBASE_SERVICE_ACCOUNT_PATH (preferred) OR FIREBASE_SERVICE_ACCOUNT_JSON (fallback)
    """
    global _firebase_app
    if _firebase_app is not None:
        return _firebase_app

    db_url = os.getenv("FIREBASE_DATABASE_URL", "").strip()
    if not db_url:
        raise RuntimeError("FIREBASE_DATABASE_URL is missing or empty")

    sa_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH", "").strip()
    sa_json = os.getenv("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()

    if sa_path:
        if not os.path.exists(sa_path):
            raise RuntimeError(f"Service account JSON not found at: {sa_path}")
        cred = credentials.Certificate(sa_path)
    elif sa_json:
        try:
            cred = credentials.Certificate(json.loads(sa_json))
        except Exception as e:
            raise RuntimeError(f"Invalid FIREBASE_SERVICE_ACCOUNT_JSON: {e}") from e
    else:
        raise RuntimeError(
            "Set FIREBASE_SERVICE_ACCOUNT_PATH (preferred) "
            "or FIREBASE_SERVICE_ACCOUNT_JSON (fallback)."
        )

    _firebase_app = firebase_admin.initialize_app(cred, {"databaseURL": db_url})
    return _firebase_app


def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _normalize_node_path(node_path: str) -> str:
    node_path = (node_path or "").strip().strip("/")
    if not node_path:
        raise ValueError("node_path cannot be empty")
    return node_path


# -------------------------------------------------------------------
# Upload mode 1: Firebase generates keys (push IDs)
# -------------------------------------------------------------------
def upload_rows_push_keys(
    *,
    node_path: str,
    rows: List[Dict[str, Any]],
    sample_keys: int = 5,
) -> Dict[str, Any]:
    """
    Upload rows under a node using Firebase-generated keys:

      /<node_path>/<pushId> = row

    Tradeoff:
      - Re-running will duplicate unless you add dedupe (we do dedupe BEFORE upload).
    """
    _init_firebase()
    node_path = _normalize_node_path(node_path)

    ref = db.reference(node_path)

    uploaded = 0
    generated_keys: List[str] = []

    for row in rows:
        new_ref = ref.push(row)
        uploaded += 1
        k = getattr(new_ref, "key", None)
        if k and len(generated_keys) < sample_keys:
            generated_keys.append(k)

    return {
        "node_path": f"/{node_path}",
        "uploaded": uploaded,
        "mode": "push_keys",
        "generated_keys_sample": generated_keys,
    }


# -------------------------------------------------------------------
# Upload mode 2: Upsert by stable key (optional)
# -------------------------------------------------------------------
def upload_rows_by_key(
    *,
    node_path: str,
    rows: List[Dict[str, Any]],
    key_field: str = "job_id",
    chunk_size: int = 250,
) -> Dict[str, Any]:
    """
    Idempotent upsert:

      /<node_path>/<row[key_field]> = row
    """
    _init_firebase()
    node_path = _normalize_node_path(node_path)

    ref = db.reference(node_path)

    uploaded = 0
    skipped = 0
    chunks = 0

    batch: Dict[str, Any] = {}
    for row in rows:
        raw_key = row.get(key_field)
        key = str(raw_key).strip() if raw_key is not None else ""
        if not key:
            skipped += 1
            continue

        batch[key] = row
        uploaded += 1

        if len(batch) >= chunk_size:
            ref.update(batch)
            chunks += 1
            batch = {}

    if batch:
        ref.update(batch)
        chunks += 1

    return {
        "node_path": f"/{node_path}",
        "uploaded": uploaded,
        "skipped_missing_key": skipped,
        "chunks": chunks,
        "mode": "upsert_by_key",
        "key_field": key_field,
    }


# -------------------------------------------------------------------
# Download node snapshot
# -------------------------------------------------------------------
def download_node_snapshot(*, node_path: str) -> Any:
    """
    Download the full node data from Firebase RTDB.
    """
    _init_firebase()
    node_path = _normalize_node_path(node_path)

    ref = db.reference(node_path)
    return ref.get()


# -------------------------------------------------------------------
# Step-0 baseline: delete local latest files, prune expired in Firebase,
# save cleaned snapshot, and return existing_job_id_set
# -------------------------------------------------------------------
@dataclass(frozen=True)
class BaselineSnapshotResult:
    ok: bool
    node_path: str
    today_ist: str
    deleted_local_files: int
    expired_keys_count: int
    expired_deleted_from_firebase: int
    kept_count: int
    saved_file: str
    existing_job_id_set: Set[int]


def _parse_deadline_yyyy_mm_dd(s: Any) -> Optional[date]:
    """
    Accepts:
      - 'YYYY-MM-DD'
      - 'YYYY-MM-DDTHH:MM:SS+05:30' (we take first 10)
    Returns date or None.
    """
    if s is None:
        return None
    txt = str(s).strip()
    if not txt:
        return None
    if len(txt) >= 10:
        txt = txt[:10]
    try:
        y, m, d = txt.split("-")
        return date(int(y), int(m), int(d))
    except Exception:
        return None


def _extract_job_id_int(job_obj: Dict[str, Any]) -> Optional[int]:
    jid = job_obj.get("job_id")
    if jid is None or str(jid).strip() == "":
        return None
    try:
        return int(jid)
    except Exception:
        return None


def _delete_all_files_in_dir(dir_path: Path) -> int:
    """
    Deletes all files in directory (not folders).
    Returns count deleted.
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
                # best-effort; don't fail pipeline for one locked file
                pass
    return deleted


def snapshot_prune_delete_and_save(
    *,
    node_path: str,
    out_dir: Path,
    deadline_field: str = "application_deadline",
    job_id_field: str = "job_id",
) -> BaselineSnapshotResult:
    """
    Step-0 baseline operation:

    1) Delete any existing files in output/extracted_latest/
    2) Download Firebase node
    3) Identify expired items where application_deadline < today (IST date)
    4) Delete expired items FROM FIREBASE
    5) Download node again (cleaned)
    6) Save cleaned snapshot JSON to output/extracted_latest/<node>_latest_<ts>.json
    7) Build existing_job_id_set from cleaned snapshot and return it (for platform-level dedupe)
    """
    _init_firebase()
    node_path = _normalize_node_path(node_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    deleted_local = _delete_all_files_in_dir(out_dir)

    # Today in local machine date; you're running in IST, and your deadlines are IST dates.
    today = date.today()
    today_str = today.isoformat()

    ref = db.reference(node_path)

    raw = ref.get()  # could be dict of pushIds or None
    if not isinstance(raw, dict):
        raw = {}

    # Determine expired push-keys to delete
    expired_keys: List[str] = []
    kept_keys: List[str] = []

    for k, v in raw.items():
        if not isinstance(v, dict):
            # Unexpected shape: keep but it won't contribute to job_id set
            kept_keys.append(k)
            continue

        dl = _parse_deadline_yyyy_mm_dd(v.get(deadline_field))
        if dl is not None and dl < today:
            expired_keys.append(k)
        else:
            kept_keys.append(k)

    # Delete expired from firebase
    expired_deleted = 0
    for k in expired_keys:
        try:
            ref.child(k).delete()
            expired_deleted += 1
        except Exception:
            # best-effort delete
            pass

    # Re-fetch cleaned snapshot (source-of-truth after delete)
    cleaned = ref.get()
    if not isinstance(cleaned, dict):
        cleaned = {}

    # Save cleaned snapshot
    safe_node = node_path.replace("/", "_") or "root"
    ts = _timestamp_str()
    file_path = out_dir / f"{safe_node}_latest_{ts}.json"
    file_path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")

    # Build existing job_id set for platform dedupe
    existing_ids: Set[int] = set()
    for _, obj in cleaned.items():
        if isinstance(obj, dict):
            jid = _extract_job_id_int(obj)
            if jid is not None:
                existing_ids.add(jid)

    return BaselineSnapshotResult(
        ok=True,
        node_path=f"/{node_path}",
        today_ist=today_str,
        deleted_local_files=deleted_local,
        expired_keys_count=len(expired_keys),
        expired_deleted_from_firebase=expired_deleted,
        kept_count=len(cleaned) if isinstance(cleaned, dict) else 0,
        saved_file=str(file_path),
        existing_job_id_set=existing_ids,
    )


__all__ = [
    "upload_rows_push_keys",
    "upload_rows_by_key",
    "download_node_snapshot",
    "snapshot_prune_delete_and_save",
    "BaselineSnapshotResult",
]
