# app/ops/firebase_client.py
from __future__ import annotations

import json
import os
from typing import Optional

import firebase_admin
from firebase_admin import credentials

_firebase_app: Optional[firebase_admin.App] = None


def init_firebase() -> firebase_admin.App:
    """
    Single source of truth for Firebase Admin init.

    Safe for:
      - FastAPI reload
      - multiple modules importing Firebase
      - multiple endpoints using RTDB

    ENV required:
      - FIREBASE_DATABASE_URL
      - FIREBASE_SERVICE_ACCOUNT_PATH (preferred) OR FIREBASE_SERVICE_ACCOUNT_JSON (fallback)
    """
    global _firebase_app

    # cached in this module
    if _firebase_app is not None:
        return _firebase_app

    # already initialized elsewhere in this process
    try:
        _firebase_app = firebase_admin.get_app()  # default app
        return _firebase_app
    except ValueError:
        pass

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
