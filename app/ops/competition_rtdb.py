# app/ops/competition_rtdb.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from firebase_admin import db

from app.ops.firebase_client import init_firebase


def _init_firebase():
    return init_firebase()


def _timestamp_str() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _normalize_node_path(node_path: str) -> str:
    node_path = (node_path or "").strip().strip("/")
    if not node_path:
        raise ValueError("node_path cannot be empty")
    return node_path


def _parse_deadline_yyyy_mm_dd(s: Any) -> Optional[date]:
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


def _coerce_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return None


def _make_comp_key(platform: Any, comp_type: Any, comp_id: Any) -> Optional[str]:
    cid = _coerce_int(comp_id)
    if cid is None:
        return None

    p = (str(platform).strip().lower() if platform is not None else "").strip()
    t = (str(comp_type).strip().lower() if comp_type is not None else "").strip()

    if not p:
        p = "unknown_platform"
    if not t:
        t = "unknown_type"

    return f"{p}:{t}:{cid}"


def _delete_all_files_in_dir(dir_path: Path) -> int:
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


def upload_rows_push_keys(
    *,
    node_path: str,
    rows: List[Dict[str, Any]],
    sample_keys: int = 5,
) -> Dict[str, Any]:
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


def download_node_snapshot(*, node_path: str) -> Any:
    _init_firebase()
    node_path = _normalize_node_path(node_path)
    return db.reference(node_path).get()


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
    existing_comp_key_set: Set[str]


def snapshot_prune_delete_and_save(
    *,
    node_path: str,
    out_dir: Path,
    deadline_field: str = "application_deadline",
) -> BaselineSnapshotResult:
    _init_firebase()
    node_path = _normalize_node_path(node_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    deleted_local = _delete_all_files_in_dir(out_dir)

    today = date.today()
    today_str = today.isoformat()

    ref = db.reference(node_path)

    raw = ref.get()
    if not isinstance(raw, dict):
        raw = {}

    expired_keys: List[str] = []
    for push_key, v in raw.items():
        if not isinstance(v, dict):
            continue
        dl = _parse_deadline_yyyy_mm_dd(v.get(deadline_field))
        if dl is not None and dl < today:
            expired_keys.append(push_key)

    expired_deleted = 0
    for push_key in expired_keys:
        try:
            ref.child(push_key).delete()
            expired_deleted += 1
        except Exception:
            pass

    cleaned = ref.get()
    if not isinstance(cleaned, dict):
        cleaned = {}

    safe_node = node_path.replace("/", "_") or "root"
    ts = _timestamp_str()
    file_path = out_dir / f"{safe_node}_latest_{ts}.json"
    file_path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")

    existing_keys: Set[str] = set()
    for _, obj in cleaned.items():
        if not isinstance(obj, dict):
            continue
        k = _make_comp_key(obj.get("platform"), obj.get("competition_type"), obj.get("competition_id"))
        if k:
            existing_keys.add(k)

    return BaselineSnapshotResult(
        ok=True,
        node_path=f"/{node_path}",
        today_ist=today_str,
        deleted_local_files=deleted_local,
        expired_keys_count=len(expired_keys),
        expired_deleted_from_firebase=expired_deleted,
        kept_count=len(cleaned) if isinstance(cleaned, dict) else 0,
        saved_file=str(file_path),
        existing_comp_key_set=existing_keys,
    )


__all__ = [
    "upload_rows_push_keys",
    "download_node_snapshot",
    "snapshot_prune_delete_and_save",
    "BaselineSnapshotResult",
]
