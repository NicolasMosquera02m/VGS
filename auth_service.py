"""Authentication, authorization, session, audit and error logging services with Firebase Firestore support."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import sqlite3
import traceback
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from firebase_backend import get_firebase_status, get_firestore_client, get_rtdb

DB_PATH = Path(__file__).with_name("app_security.db")
DEFAULT_TOKEN_TTL_MINUTES = 30
DEFAULT_SESSION_IDLE_MINUTES = 60
MAX_FAILED_ATTEMPTS = 5


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime | None = None) -> str:
    val = value or _now()
    return val.astimezone(timezone.utc).isoformat()


def _connect() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


@contextmanager
def _db():
    connection = _connect()
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def _hash_password(password: str, salt: bytes | None = None) -> str:
    salt = salt or os.urandom(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000)
    return f"pbkdf2_sha256$310000${salt.hex()}${digest.hex()}"


def _verify_password(password: str, encoded: str) -> bool:
    try:
        algorithm, rounds, salt_hex, digest_hex = encoded.split("$")
        if algorithm != "pbkdf2_sha256":
            return False
        candidate = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt_hex), int(rounds)
        )
        return hmac.compare_digest(candidate.hex(), digest_hex)
    except (TypeError, ValueError):
        return False


def validate_password(password: str) -> str | None:
    if len(password) < 8:
        return "La contraseña debe tener al menos 8 caracteres."
    if not any(char.isupper() for char in password):
        return "La contraseña debe incluir una letra mayúscula."
    if not any(char.islower() for char in password):
        return "La contraseña debe incluir una letra minúscula."
    if not any(char.isdigit() for char in password):
        return "La contraseña debe incluir un número."
    if not any(not char.isalnum() for char in password):
        return "La contraseña debe incluir un carácter especial."
    return None


def validate_username(username: str) -> str | None:
    if not username or len(username) > 50:
        return "El usuario debe tener entre 1 y 50 caracteres."
    if not all(char.isalnum() or char in "._-" for char in username):
        return "El usuario solo puede contener letras, números, punto, guion y guion bajo."
    return None


def _sync_firestore_doc(collection: str, doc_id: str, data: dict[str, Any]) -> None:
    # 1. Firebase Realtime Database
    rtdb = get_rtdb()
    if rtdb is not None:
        try:
            rtdb.reference(f"{collection}/{doc_id}").update(data)
        except Exception:
            try:
                rtdb.reference(f"{collection}/{doc_id}").set(data)
            except Exception as exc:
                print(f"[RTDB Warning] Error updating {collection}/{doc_id}: {exc}")

    # 2. Firebase Cloud Firestore (if provisioned)
    db = get_firestore_client()
    if db is not None:
        try:
            db.collection(collection).document(str(doc_id)).set(data, merge=True)
        except Exception as exc:
            pass


def _add_firestore_doc(collection: str, data: dict[str, Any]) -> None:
    # 1. Firebase Realtime Database
    rtdb = get_rtdb()
    if rtdb is not None:
        try:
            rtdb.reference(collection).push(data)
        except Exception as exc:
            print(f"[RTDB Warning] Error adding to {collection}: {exc}")

    # 2. Firebase Cloud Firestore (if provisioned)
    db = get_firestore_client()
    if db is not None:
        try:
            db.collection(collection).add(data)
        except Exception as exc:
            pass


def init_db() -> None:
    with _db() as connection:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL UNIQUE COLLATE NOCASE,
                email TEXT NOT NULL UNIQUE COLLATE NOCASE,
                password_hash TEXT NOT NULL,
                failed_attempts INTEGER NOT NULL DEFAULT 0,
                is_locked INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS roles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                description TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS permissions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE COLLATE NOCASE,
                description TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS user_roles (
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, role_id)
            );
            CREATE TABLE IF NOT EXISTS role_permissions (
                role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
                permission_id INTEGER NOT NULL REFERENCES permissions(id) ON DELETE CASCADE,
                PRIMARY KEY (role_id, permission_id)
            );
            CREATE TABLE IF NOT EXISTS sessions (
                token_hash TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS password_reset_tokens (
                token_hash TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                expires_at TEXT NOT NULL,
                used INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                action TEXT NOT NULL,
                target TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS error_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                actor_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                traceback TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS app_settings (
                name TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        _setting(connection, "token_ttl_minutes", str(DEFAULT_TOKEN_TTL_MINUTES))
        _setting(connection, "session_idle_minutes", str(DEFAULT_SESSION_IDLE_MINUTES))
        for name, description in (
            ("dashboard.view", "Acceder al dashboard"),
            ("users.manage", "Crear y modificar usuarios"),
            ("roles.manage", "Crear roles y permisos"),
            ("audit.view", "Consultar auditoría"),
            ("profile.edit", "Editar el perfil propio"),
            ("files.upload", "Cargar archivos"),
        ):
            connection.execute(
                "INSERT OR IGNORE INTO permissions(name, description) VALUES (?, ?)",
                (name, description),
            )
        connection.execute(
            "INSERT OR IGNORE INTO roles(name, description) VALUES (?, ?)",
            ("admin", "Administrador de la plataforma"),
        )
        connection.execute(
            "INSERT OR IGNORE INTO roles(name, description) VALUES (?, ?)",
            ("user", "Usuario estándar"),
        )
        admin_role = connection.execute(
            "SELECT id FROM roles WHERE name='admin'"
        ).fetchone()["id"]
        connection.execute(
            """
            INSERT OR IGNORE INTO role_permissions(role_id, permission_id)
            SELECT ?, id FROM permissions
            """,
            (admin_role,),
        )
        user_role = connection.execute(
            "SELECT id FROM roles WHERE name='user'"
        ).fetchone()["id"]
        connection.execute(
            """
            INSERT OR IGNORE INTO role_permissions(role_id, permission_id)
            SELECT ?, id FROM permissions WHERE name IN ('dashboard.view', 'profile.edit')
            """,
            (user_role,),
        )
        if not connection.execute("SELECT 1 FROM users LIMIT 1").fetchone():
            now = _iso(_now())
            cursor = connection.execute(
                """
                INSERT INTO users(username, email, password_hash, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                ("admin", "admin@example.com", _hash_password("Admin123!"), now, now),
            )
            admin_id = cursor.lastrowid
            connection.execute(
                "INSERT INTO user_roles(user_id, role_id) VALUES (?, ?)",
                (admin_id, admin_role),
            )
            _audit(connection, admin_id, "bootstrap.admin_created", "admin", "Cuenta inicial")

    # If Firebase is available, ensure cloud seed
    _sync_settings_to_firebase()


def _setting(connection: sqlite3.Connection, name: str, value: str) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO app_settings(name, value) VALUES (?, ?)", (name, value)
    )


def _sync_settings_to_firebase() -> None:
    status = get_firebase_status()
    if status.get("connected"):
        settings = get_settings()
        _sync_firestore_doc("app_settings", "global_policy", settings)


def _audit(connection: sqlite3.Connection, actor_id: int | None, action: str, target: str = "", details: str = "") -> None:
    now_iso = _iso(_now())
    connection.execute(
        "INSERT INTO audit_logs(actor_user_id, action, target, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (actor_id, action, target, details, now_iso),
    )
    # Mirror to Firebase
    _add_firestore_doc("audit_logs", {
        "actor_user_id": actor_id,
        "action": action,
        "target": target,
        "details": details,
        "created_at": now_iso,
    })


def log_error(error_type: str, message: str, traceback_str: str = "", actor_id: int | None = None) -> None:
    now_iso = _iso(_now())
    with _db() as connection:
        connection.execute(
            "INSERT INTO error_logs(actor_user_id, error_type, message, traceback, created_at) VALUES (?, ?, ?, ?, ?)",
            (actor_id, error_type, message, traceback_str, now_iso),
        )
    # Mirror to Firebase
    _add_firestore_doc("error_logs", {
        "actor_user_id": actor_id,
        "error_type": error_type,
        "message": message,
        "traceback": traceback_str,
        "created_at": now_iso,
    })


def list_error_logs(limit: int = 100) -> list[dict]:
    with _db() as connection:
        rows = connection.execute(
            """
            SELECT e.id, e.created_at, COALESCE(u.username, 'sistema') actor, e.error_type, e.message, e.traceback
            FROM error_logs e LEFT JOIN users u ON u.id=e.actor_user_id
            ORDER BY e.id DESC LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]


def _token_hash(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def _user_query() -> str:
    return """
        SELECT u.id, u.username, u.email, u.password_hash, u.failed_attempts, u.is_locked,
               GROUP_CONCAT(DISTINCT r.name) AS roles
        FROM users u LEFT JOIN user_roles ur ON ur.user_id=u.id
        LEFT JOIN roles r ON r.id=ur.role_id
    """


def get_user(user_id: int) -> dict | None:
    with _db() as connection:
        row = connection.execute(_user_query() + " WHERE u.id=? GROUP BY u.id", (user_id,)).fetchone()
        return dict(row) if row else None


def authenticate(identifier: str, password: str) -> tuple[dict | None, str]:
    with _db() as connection:
        row = connection.execute(
            _user_query() + " WHERE u.username=? OR u.email=? GROUP BY u.id",
            (identifier.strip(), identifier.strip()),
        ).fetchone()
        if not row:
            return None, "Credenciales inválidas."
        if row["is_locked"]:
            return None, "La cuenta está bloqueada después de 5 intentos fallidos."
        if not _verify_password(password, row["password_hash"]):
            attempts = row["failed_attempts"] + 1
            locked = int(attempts >= MAX_FAILED_ATTEMPTS)
            now_iso = _iso(_now())
            connection.execute(
                "UPDATE users SET failed_attempts=?, is_locked=?, updated_at=? WHERE id=?",
                (attempts, locked, now_iso, row["id"]),
            )
            _audit(connection, row["id"], "auth.login_failed", row["username"], f"Intento fallido {attempts}/5")
            _sync_firestore_doc("users", str(row["id"]), {
                "failed_attempts": attempts,
                "is_locked": locked,
                "updated_at": now_iso,
            })
            return None, "Credenciales inválidas." if not locked else "Cuenta bloqueada después de 5 intentos."
        connection.execute(
            "UPDATE users SET failed_attempts=0, updated_at=? WHERE id=?", (_iso(_now()), row["id"])
        )
        _audit(connection, row["id"], "auth.login_success", row["username"])
        _sync_firestore_doc("users", str(row["id"]), {
            "failed_attempts": 0,
            "updated_at": _iso(_now()),
        })
        return dict(row), ""


def create_session(user_id: int) -> str:
    with _db() as connection:
        ttl = int(connection.execute(
            "SELECT value FROM app_settings WHERE name='token_ttl_minutes'"
        ).fetchone()["value"])
        now = _now()
        token = secrets.token_urlsafe(32)
        thash = _token_hash(token)
        now_iso = _iso(now)
        exp_iso = _iso(now + timedelta(minutes=ttl))
        connection.execute(
            "INSERT INTO sessions(token_hash, user_id, created_at, last_seen_at, expires_at) VALUES (?, ?, ?, ?, ?)",
            (thash, user_id, now_iso, now_iso, exp_iso),
        )
        _audit(connection, user_id, "auth.session_created")
        _sync_firestore_doc("sessions", thash, {
            "token_hash": thash,
            "user_id": user_id,
            "created_at": now_iso,
            "last_seen_at": now_iso,
            "expires_at": exp_iso,
            "revoked": 0,
        })
        return token


def get_session(token: str | None) -> dict | None:
    if not token:
        return None
    thash = _token_hash(token)
    with _db() as connection:
        row = connection.execute(
            """
            SELECT s.*, u.username, u.email FROM sessions s JOIN users u ON u.id=s.user_id
            WHERE s.token_hash=? AND s.revoked=0
            """,
            (thash,),
        ).fetchone()
        if not row:
            return None
        now = _now()
        if datetime.fromisoformat(row["expires_at"]) <= now:
            connection.execute("UPDATE sessions SET revoked=1 WHERE token_hash=?", (thash,))
            _sync_firestore_doc("sessions", thash, {"revoked": 1})
            return None
        idle = int(connection.execute(
            "SELECT value FROM app_settings WHERE name='session_idle_minutes'"
        ).fetchone()["value"])
        if datetime.fromisoformat(row["last_seen_at"]) + timedelta(minutes=idle) <= now:
            connection.execute("UPDATE sessions SET revoked=1 WHERE token_hash=?", (thash,))
            _audit(connection, row["user_id"], "auth.session_expired")
            _sync_firestore_doc("sessions", thash, {"revoked": 1})
            return None
        connection.execute("UPDATE sessions SET last_seen_at=? WHERE token_hash=?", (_iso(now), thash))
        _sync_firestore_doc("sessions", thash, {"last_seen_at": _iso(now)})
        return {"id": row["user_id"], "username": row["username"], "email": row["email"], "token": token}


def revoke_session(token: str | None, user_id: int | None = None) -> None:
    if not token:
        return
    thash = _token_hash(token)
    with _db() as connection:
        connection.execute("UPDATE sessions SET revoked=1 WHERE token_hash=?", (thash,))
        _audit(connection, user_id, "auth.logout")
        _sync_firestore_doc("sessions", thash, {"revoked": 1})


def has_permission(user_id: int, permission: str) -> bool:
    with _db() as connection:
        return bool(connection.execute(
            """
            SELECT 1 FROM user_roles ur JOIN role_permissions rp ON rp.role_id=ur.role_id
            JOIN permissions p ON p.id=rp.permission_id
            WHERE ur.user_id=? AND p.name=?
            """, (user_id, permission)
        ).fetchone())


def list_users() -> list[dict]:
    with _db() as connection:
        return [dict(row) for row in connection.execute(
            _user_query() + " GROUP BY u.id ORDER BY u.username"
        ).fetchall()]


def list_roles() -> list[dict]:
    with _db() as connection:
        rows = connection.execute("""
            SELECT r.id, r.name, r.description, GROUP_CONCAT(p.name, ', ') permissions
            FROM roles r LEFT JOIN role_permissions rp ON rp.role_id=r.id
            LEFT JOIN permissions p ON p.id=rp.permission_id GROUP BY r.id ORDER BY r.name
        """).fetchall()
        return [dict(row) for row in rows]


def list_permissions() -> list[dict]:
    with _db() as connection:
        return [dict(row) for row in connection.execute("SELECT * FROM permissions ORDER BY name").fetchall()]


def create_user(username: str, email: str, password: str, role_ids: list[int], actor_id: int | None = None) -> tuple[bool, str]:
    error = validate_username(username) or validate_password(password)
    if error:
        return False, error
    if "@" not in email or len(email) > 254:
        return False, "Ingresa un correo electrónico válido."
    with _db() as connection:
        try:
            now = _iso(_now())
            pw_hash = _hash_password(password)
            cursor = connection.execute(
                "INSERT INTO users(username,email,password_hash,created_at,updated_at) VALUES (?,?,?,?,?)",
                (username.strip(), email.strip().lower(), pw_hash, now, now),
            )
            new_id = cursor.lastrowid
            for role_id in role_ids:
                connection.execute("INSERT INTO user_roles(user_id, role_id) VALUES (?, ?)", (new_id, role_id))
            _audit(connection, actor_id, "user.created", username, f"roles={role_ids}")
            
            # Mirror to Firestore
            _sync_firestore_doc("users", str(new_id), {
                "id": new_id,
                "username": username.strip(),
                "email": email.strip().lower(),
                "password_hash": pw_hash,
                "failed_attempts": 0,
                "is_locked": 0,
                "created_at": now,
                "updated_at": now,
                "role_ids": role_ids,
            })
            return True, "Usuario creado exitosamente."
        except sqlite3.IntegrityError:
            return False, "El usuario o correo electrónico ya está registrado."


def update_profile(user_id: int, email: str, password: str, actor_id: int) -> tuple[bool, str]:
    if "@" not in email or len(email) > 254:
        return False, "Ingresa un correo electrónico válido."
    if password:
        error = validate_password(password)
        if error:
            return False, error
    with _db() as connection:
        try:
            now = _iso(_now())
            values = [email.strip().lower(), now, user_id]
            query = "UPDATE users SET email=?, updated_at=?"
            firestore_updates: dict[str, Any] = {"email": email.strip().lower(), "updated_at": now}
            if password:
                pw_hash = _hash_password(password)
                query += ", password_hash=?"
                values.insert(-1, pw_hash)
                firestore_updates["password_hash"] = pw_hash
            connection.execute(query + " WHERE id=?", values)
            _audit(connection, actor_id, "profile.updated", str(user_id), "email/password actualizados")
            _sync_firestore_doc("users", str(user_id), firestore_updates)
            return True, "Perfil actualizado."
        except sqlite3.IntegrityError:
            return False, "El correo electrónico ya está registrado."


def change_user_password(
    user_id: int, password: str, actor_id: int, allow_override: bool = False
) -> tuple[bool, str]:
    """Modifies password. If allow_override is False, enforces password policy (e.g. min 8 chars)."""
    if not allow_override:
        error = validate_password(password)
        if error:
            with _db() as connection:
                _audit(
                    connection,
                    actor_id,
                    "user.password_change_rejected",
                    str(user_id),
                    f"Rechazado por política: longitud={len(password)}",
                )
            return False, error

    now = _iso(_now())
    pw_hash = _hash_password(password)
    with _db() as connection:
        connection.execute(
            "UPDATE users SET password_hash=?, updated_at=? WHERE id=?",
            (pw_hash, now, user_id),
        )
        _audit(connection, actor_id, "user.password_changed", str(user_id), f"override={allow_override}")
    _sync_firestore_doc("users", str(user_id), {"password_hash": pw_hash, "updated_at": now})
    return True, "Contraseña actualizada."


def create_role(name: str, description: str, permission_ids: list[int], actor_id: int) -> tuple[bool, str]:
    if not name.strip() or len(name) > 50:
        return False, "El nombre del rol es obligatorio y debe tener máximo 50 caracteres."
    with _db() as connection:
        try:
            cursor = connection.execute("INSERT INTO roles(name,description) VALUES (?,?)", (name.strip(), description.strip()))
            role_id = cursor.lastrowid
            for permission_id in permission_ids:
                connection.execute("INSERT INTO role_permissions(role_id,permission_id) VALUES (?,?)", (role_id, permission_id))
            _audit(connection, actor_id, "role.created", name, f"permissions={permission_ids}")
            _sync_firestore_doc("roles", str(role_id), {
                "id": role_id,
                "name": name.strip(),
                "description": description.strip(),
                "permission_ids": permission_ids,
            })
            return True, "Rol creado."
        except sqlite3.IntegrityError:
            return False, "Ese rol ya existe."


def update_role_permissions(role_id: int, permission_ids: list[int], actor_id: int) -> tuple[bool, str]:
    """Updates the entire set of permissions for an existing role."""
    with _db() as connection:
        role = connection.execute("SELECT id, name FROM roles WHERE id=?", (role_id,)).fetchone()
        if not role:
            return False, "El rol no existe."
        connection.execute("DELETE FROM role_permissions WHERE role_id=?", (role_id,))
        connection.executemany(
            "INSERT INTO role_permissions(role_id, permission_id) VALUES (?, ?)",
            [(role_id, pid) for pid in permission_ids],
        )
        _audit(connection, actor_id, "role.permissions_updated", role["name"], f"permissions={permission_ids}")
        _sync_firestore_doc("roles", str(role_id), {"permission_ids": permission_ids})
        return True, f"Permisos del rol '{role['name']}' actualizados correctamente."


def assign_roles(user_id: int, role_ids: list[int], actor_id: int) -> None:
    with _db() as connection:
        connection.execute("DELETE FROM user_roles WHERE user_id=?", (user_id,))
        connection.executemany("INSERT INTO user_roles(user_id,role_id) VALUES (?,?)", [(user_id, rid) for rid in role_ids])
        _audit(connection, actor_id, "user.roles_updated", str(user_id), f"roles={role_ids}")
        _sync_firestore_doc("users", str(user_id), {"role_ids": role_ids})


def request_password_reset_details(identifier: str) -> tuple[str | None, str, str]:
    """Generates a password reset token and returns (token, email, username)."""
    with _db() as connection:
        row = connection.execute(
            "SELECT id, username, email FROM users WHERE username=? OR email=?",
            (identifier.strip(), identifier.strip()),
        ).fetchone()
        if not row:
            return None, "", ""
        token = secrets.token_urlsafe(24)
        thash = _token_hash(token)
        exp_iso = _iso(_now() + timedelta(minutes=15))
        connection.execute(
            "INSERT INTO password_reset_tokens(token_hash,user_id,expires_at) VALUES (?,?,?)",
            (thash, row["id"], exp_iso),
        )
        _audit(connection, row["id"], "auth.password_reset_requested")
        _sync_firestore_doc("password_reset_tokens", thash, {
            "token_hash": thash,
            "user_id": row["id"],
            "expires_at": exp_iso,
            "used": 0,
        })
        return token, row["email"], row["username"]


def request_password_reset(identifier: str) -> str | None:
    token, _, _ = request_password_reset_details(identifier)
    return token


def consume_password_reset(token: str, password: str) -> tuple[bool, str]:
    error = validate_password(password)
    if error:
        return False, error
    thash = _token_hash(token)
    with _db() as connection:
        row = connection.execute(
            "SELECT user_id, expires_at, used FROM password_reset_tokens WHERE token_hash=?",
            (thash,),
        ).fetchone()
        if not row or row["used"] or datetime.fromisoformat(row["expires_at"]) <= _now():
            return False, "El enlace de recuperación no es válido o expiró."
        now = _iso(_now())
        pw_hash = _hash_password(password)
        connection.execute("UPDATE users SET password_hash=?, failed_attempts=0, is_locked=0, updated_at=? WHERE id=?", (pw_hash, now, row["user_id"]))
        connection.execute("UPDATE password_reset_tokens SET used=1 WHERE token_hash=?", (thash,))
        _audit(connection, row["user_id"], "auth.password_reset_completed")
        _sync_firestore_doc("users", str(row["user_id"]), {
            "password_hash": pw_hash,
            "failed_attempts": 0,
            "is_locked": 0,
            "updated_at": now,
        })
        _sync_firestore_doc("password_reset_tokens", thash, {"used": 1})
        return True, "Contraseña restablecida."


def update_settings(token_ttl: int, session_idle: int, actor_id: int) -> None:
    with _db() as connection:
        connection.execute("UPDATE app_settings SET value=? WHERE name='token_ttl_minutes'", (str(token_ttl),))
        connection.execute("UPDATE app_settings SET value=? WHERE name='session_idle_minutes'", (str(session_idle),))
        _audit(connection, actor_id, "settings.session_policy_updated", details=f"token={token_ttl};idle={session_idle}")
        _sync_firestore_doc("app_settings", "global_policy", {
            "token_ttl_minutes": token_ttl,
            "session_idle_minutes": session_idle,
            "updated_at": _iso(_now()),
        })


def get_settings() -> dict[str, int]:
    with _db() as connection:
        return {row["name"]: int(row["value"]) for row in connection.execute("SELECT name,value FROM app_settings")}


def audit_logs(limit: int = 100) -> list[dict]:
    with _db() as connection:
        return [dict(row) for row in connection.execute(
            """
            SELECT a.created_at, COALESCE(u.username, 'sistema') actor, a.action, a.target, a.details
            FROM audit_logs a LEFT JOIN users u ON u.id=a.actor_user_id
            ORDER BY a.id DESC LIMIT ?
            """, (limit,)
        ).fetchall()]


def get_raw_database_inspection() -> list[dict]:
    """Returns database user records showing password_hash encryption (PBKDF2-SHA256) for audit and security testing."""
    with _db() as connection:
        rows = connection.execute(
            """
            SELECT id, username, email, password_hash, failed_attempts, is_locked, created_at, updated_at
            FROM users ORDER BY id
            """
        ).fetchall()
        return [dict(row) for row in rows]


def sync_sqlite_to_firebase() -> tuple[bool, str]:
    """Uploads current local database state (users, roles, permissions, settings, audit & error logs) to Firebase (Realtime Database & Firestore)."""
    status = get_firebase_status()
    if not status.get("connected"):
        return False, f"Firebase no está conectado: {status.get('error_message')}"

    rtdb = get_rtdb()
    fs = get_firestore_client()

    if rtdb is None and fs is None:
        return False, "No se pudo inicializar ningún cliente de base de datos de Firebase."

    try:
        with _db() as connection:
            # Users
            users = [dict(r) for r in connection.execute("SELECT * FROM users").fetchall()]
            users_dict = {}
            for u in users:
                user_roles = [r["role_id"] for r in connection.execute("SELECT role_id FROM user_roles WHERE user_id=?", (u["id"],)).fetchall()]
                u_data = dict(u)
                u_data["role_ids"] = user_roles
                users_dict[str(u["id"])] = u_data
                if fs is not None:
                    try:
                        fs.collection("users").document(str(u["id"])).set(u_data, merge=True)
                    except Exception:
                        pass
            if rtdb is not None:
                rtdb.reference("users").set(users_dict)

            # Roles
            roles = [dict(r) for r in connection.execute("SELECT * FROM roles").fetchall()]
            roles_dict = {}
            for r in roles:
                perm_ids = [p["permission_id"] for p in connection.execute("SELECT permission_id FROM role_permissions WHERE role_id=?", (r["id"],)).fetchall()]
                r_data = dict(r)
                r_data["permission_ids"] = perm_ids
                roles_dict[str(r["id"])] = r_data
                if fs is not None:
                    try:
                        fs.collection("roles").document(str(r["id"])).set(r_data, merge=True)
                    except Exception:
                        pass
            if rtdb is not None:
                rtdb.reference("roles").set(roles_dict)

            # Permissions
            permissions = [dict(p) for p in connection.execute("SELECT * FROM permissions").fetchall()]
            perms_dict = {str(p["id"]): dict(p) for p in permissions}
            if rtdb is not None:
                rtdb.reference("permissions").set(perms_dict)
            if fs is not None:
                for p in permissions:
                    try:
                        fs.collection("permissions").document(str(p["id"])).set(dict(p), merge=True)
                    except Exception:
                        pass

            # App settings
            settings = {r["name"]: int(r["value"]) for r in connection.execute("SELECT name, value FROM app_settings").fetchall()}
            if rtdb is not None:
                rtdb.reference("app_settings/global_policy").set(settings)
            if fs is not None:
                try:
                    fs.collection("app_settings").document("global_policy").set(settings, merge=True)
                except Exception:
                    pass

            # Audit logs (most recent 50)
            logs = [dict(l) for l in connection.execute("SELECT * FROM audit_logs ORDER BY id DESC LIMIT 50").fetchall()]
            logs_dict = {str(l["id"]): dict(l) for l in logs}
            if rtdb is not None:
                rtdb.reference("audit_logs").set(logs_dict)
            if fs is not None:
                for l in logs:
                    try:
                        fs.collection("audit_logs").document(str(l["id"])).set(dict(l), merge=True)
                    except Exception:
                        pass

            # Error logs
            errs = [dict(e) for e in connection.execute("SELECT * FROM error_logs ORDER BY id DESC LIMIT 50").fetchall()]
            errs_dict = {str(e["id"]): dict(e) for e in errs}
            if rtdb is not None:
                rtdb.reference("error_logs").set(errs_dict)
            if fs is not None:
                for e in errs:
                    try:
                        fs.collection("error_logs").document(str(e["id"])).set(dict(e), merge=True)
                    except Exception:
                        pass

        dest = "Firebase Realtime Database & Firestore" if fs else "Firebase Realtime Database"
        return True, f"¡Sincronización exitosa con {dest}! {len(users)} usuarios, {len(roles)} roles, {len(logs)} logs y configuraciones cargadas."
    except Exception as exc:
        trace = traceback.format_exc()
        log_error("FirebaseSyncError", str(exc), trace)
        return False, f"Fallo al sincronizar con Firebase: {exc}"
