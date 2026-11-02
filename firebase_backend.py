"""Firebase integration (Realtime Database & Firestore) for VGS Security System."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

CREDENTIAL_CANDIDATES = [
    "serviceAccountKey.json",
    "firebase_credentials.json",
    "firebase-key.json",
    "firebase_key.json",
    "firebase-adminsdk.json",
]

DEFAULT_PROJECT_ID = "videogamestudiovgs"
DEFAULT_DATABASE_URL = "https://videogamestudiovgs-default-rtdb.firebaseio.com"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    val = value or _now()
    return val.astimezone(timezone.utc).isoformat()


_firebase_app = None
_rtdb = None
_firestore_db = None
_firebase_auth = None
_initialization_status: dict[str, Any] = {
    "connected": False,
    "credentials_file": None,
    "database_type": None,
    "error_message": None,
    "project_id": DEFAULT_PROJECT_ID,
}


def find_credentials_path() -> Path | None:
    env_path = os.getenv("FIREBASE_CREDENTIALS_PATH") or os.getenv("FIREBASE_CREDENTIALS")
    if env_path and Path(env_path).exists():
        return Path(env_path)

    project_root = Path(__file__).resolve().parent
    for candidate in CREDENTIAL_CANDIDATES:
        p = project_root / candidate
        if p.exists():
            return p

    for p in project_root.glob("*.json"):
        name_lower = p.name.lower()
        if "videogamestudiovgs" in name_lower or "firebase-adminsdk" in name_lower:
            return p

    return None


def get_firebase_status() -> dict[str, Any]:
    global _initialization_status
    if not _initialization_status["connected"]:
        init_firebase()
    return dict(_initialization_status)


def init_firebase(force_reinit: bool = False):
    global _firebase_app, _rtdb, _firestore_db, _firebase_auth, _initialization_status

    if _firebase_app is not None and not force_reinit:
        return _firebase_app

    cred_path = find_credentials_path()
    if not cred_path:
        _initialization_status = {
            "connected": False,
            "credentials_file": None,
            "database_type": None,
            "error_message": "No se encontró ningún archivo de credenciales (serviceAccountKey.json).",
            "project_id": DEFAULT_PROJECT_ID,
        }
        return None

    try:
        import firebase_admin
        from firebase_admin import auth, credentials, db, firestore

        if not firebase_admin._apps:
            cred = credentials.Certificate(str(cred_path))
            _firebase_app = firebase_admin.initialize_app(
                cred,
                {
                    "projectId": DEFAULT_PROJECT_ID,
                    "databaseURL": DEFAULT_DATABASE_URL,
                },
            )
        else:
            _firebase_app = firebase_admin.get_app()

        _firebase_auth = auth
        _rtdb = db

        # Test RTDB or Firestore
        active_type = "Realtime Database"
        try:
            _firestore_db = firestore.client()
        except Exception:
            _firestore_db = None

        _initialization_status = {
            "connected": True,
            "credentials_file": str(cred_path.name),
            "database_type": active_type,
            "database_url": DEFAULT_DATABASE_URL,
            "error_message": None,
            "project_id": DEFAULT_PROJECT_ID,
        }
        return _firebase_app
    except Exception as exc:
        _initialization_status = {
            "connected": False,
            "credentials_file": str(cred_path.name) if cred_path else None,
            "database_type": None,
            "error_message": f"Error al inicializar Firebase: {exc}",
            "project_id": DEFAULT_PROJECT_ID,
        }
        return None


def get_rtdb():
    init_firebase()
    return _rtdb


def get_firestore_client():
    init_firebase()
    return _firestore_db


def get_firebase_auth():
    init_firebase()
    return _firebase_auth
