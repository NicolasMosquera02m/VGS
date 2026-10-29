"""Authentication, authorization, session and audit services for the Streamlit app."""

from __future__ import annotations

import hashlib
import hmac
import os
import secrets
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

DB_PATH = Path(__file__).with_name("app_security.db")
DEFAULT_TOKEN_TTL_MINUTES = 30
DEFAULT_SESSION_IDLE_MINUTES = 60
MAX_FAILED_ATTEMPTS = 5


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


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
            connection.execute(
                "INSERT INTO user_roles(user_id, role_id) VALUES (?, ?)",
                (cursor.lastrowid, admin_role),
            )
            _audit(connection, cursor.lastrowid, "bootstrap.admin_created", "admin", "Cuenta inicial")


def _setting(connection: sqlite3.Connection, name: str, value: str) -> None:
    connection.execute(
        "INSERT OR IGNORE INTO app_settings(name, value) VALUES (?, ?)", (name, value)
    )


def _audit(connection: sqlite3.Connection, actor_id: int | None, action: str, target: str = "", details: str = "") -> None:
    connection.execute(
        "INSERT INTO audit_logs(actor_user_id, action, target, details, created_at) VALUES (?, ?, ?, ?, ?)",
        (actor_id, action, target, details, _iso(_now())),
    )


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
            connection.execute(
                "UPDATE users SET failed_attempts=?, is_locked=?, updated_at=? WHERE id=?",
                (attempts, locked, _iso(_now()), row["id"]),
            )
            _audit(connection, row["id"], "auth.login_failed", row["username"], f"Intento {attempts}")
            return None, "Credenciales inválidas." if not locked else "Cuenta bloqueada después de 5 intentos."
        connection.execute(
            "UPDATE users SET failed_attempts=0, updated_at=? WHERE id=?", (_iso(_now()), row["id"])
        )
        _audit(connection, row["id"], "auth.login_success", row["username"])
        return dict(row), ""


def create_session(user_id: int) -> str:
    with _db() as connection:
        ttl = int(connection.execute(
            "SELECT value FROM app_settings WHERE name='token_ttl_minutes'"
        ).fetchone()["value"])
        now = _now()
        token = secrets.token_urlsafe(32)
        connection.execute(
            "INSERT INTO sessions(token_hash, user_id, created_at, last_seen_at, expires_at) VALUES (?, ?, ?, ?, ?)",
            (_token_hash(token), user_id, _iso(now), _iso(now), _iso(now + timedelta(minutes=ttl))),
        )
        _audit(connection, user_id, "auth.session_created")
        return token


def get_session(token: str | None) -> dict | None:
    if not token:
        return None
    with _db() as connection:
        row = connection.execute(
            """
            SELECT s.*, u.username, u.email FROM sessions s JOIN users u ON u.id=s.user_id
            WHERE s.token_hash=? AND s.revoked=0
            """,
            (_token_hash(token),),
        ).fetchone()
        if not row:
            return None
        now = _now()
        if datetime.fromisoformat(row["expires_at"]) <= now:
            connection.execute("UPDATE sessions SET revoked=1 WHERE token_hash=?", (_token_hash(token),))
            return None
        idle = int(connection.execute(
            "SELECT value FROM app_settings WHERE name='session_idle_minutes'"
        ).fetchone()["value"])
        if datetime.fromisoformat(row["last_seen_at"]) + timedelta(minutes=idle) <= now:
            connection.execute("UPDATE sessions SET revoked=1 WHERE token_hash=?", (_token_hash(token),))
            _audit(connection, row["user_id"], "auth.session_expired")
            return None
        connection.execute("UPDATE sessions SET last_seen_at=? WHERE token_hash=?", (_iso(now), _token_hash(token)))
        return {"id": row["user_id"], "username": row["username"], "email": row["email"], "token": token}


def revoke_session(token: str | None, user_id: int | None = None) -> None:
    if not token:
        return
    with _db() as connection:
        connection.execute("UPDATE sessions SET revoked=1 WHERE token_hash=?", (_token_hash(token),))
        _audit(connection, user_id, "auth.logout")


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


def create_user(username: str, email: str, password: str, role_ids: list[int], actor_id: int) -> tuple[bool, str]:
    error = validate_username(username) or validate_password(password)
    if error:
        return False, error
    if "@" not in email or len(email) > 254:
        return False, "Ingresa un correo electrónico válido."
    with _db() as connection:
        try:
            now = _iso(_now())
            cursor = connection.execute(
                "INSERT INTO users(username,email,password_hash,created_at,updated_at) VALUES (?,?,?,?,?)",
                (username.strip(), email.strip().lower(), _hash_password(password), now, now),
            )
            for role_id in role_ids:
                connection.execute("INSERT INTO user_roles(user_id, role_id) VALUES (?, ?)", (cursor.lastrowid, role_id))
            _audit(connection, actor_id, "user.created", username, f"roles={role_ids}")
            return True, "Usuario creado."
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
            values = [email.strip().lower(), _iso(_now()), user_id]
            query = "UPDATE users SET email=?, updated_at=?"
            if password:
                query += ", password_hash=?"
                values.insert(-1, _hash_password(password))
            connection.execute(query + " WHERE id=?", values)
            _audit(connection, actor_id, "profile.updated", str(user_id), "email/password actualizados")
            return True, "Perfil actualizado."
        except sqlite3.IntegrityError:
            return False, "El correo electrónico ya está registrado."


def change_user_password(user_id: int, password: str, actor_id: int) -> tuple[bool, str]:
    error = validate_password(password)
    if error:
        return False, error
    with _db() as connection:
        connection.execute("UPDATE users SET password_hash=?, updated_at=? WHERE id=?", (_hash_password(password), _iso(_now()), user_id))
        _audit(connection, actor_id, "user.password_changed", str(user_id))
    return True, "Contraseña actualizada."


def create_role(name: str, description: str, permission_ids: list[int], actor_id: int) -> tuple[bool, str]:
    if not name.strip() or len(name) > 50:
        return False, "El nombre del rol es obligatorio y debe tener máximo 50 caracteres."
    with _db() as connection:
        try:
            cursor = connection.execute("INSERT INTO roles(name,description) VALUES (?,?)", (name.strip(), description.strip()))
            for permission_id in permission_ids:
                connection.execute("INSERT INTO role_permissions(role_id,permission_id) VALUES (?,?)", (cursor.lastrowid, permission_id))
            _audit(connection, actor_id, "role.created", name, f"permissions={permission_ids}")
            return True, "Rol creado."
        except sqlite3.IntegrityError:
            return False, "Ese rol ya existe."


def assign_roles(user_id: int, role_ids: list[int], actor_id: int) -> None:
    with _db() as connection:
        connection.execute("DELETE FROM user_roles WHERE user_id=?", (user_id,))
        connection.executemany("INSERT INTO user_roles(user_id,role_id) VALUES (?,?)", [(user_id, rid) for rid in role_ids])
        _audit(connection, actor_id, "user.roles_updated", str(user_id), f"roles={role_ids}")


def request_password_reset(identifier: str) -> str | None:
    with _db() as connection:
        row = connection.execute("SELECT id FROM users WHERE username=? OR email=?", (identifier.strip(), identifier.strip())).fetchone()
        if not row:
            return None
        token = secrets.token_urlsafe(24)
        connection.execute(
            "INSERT INTO password_reset_tokens(token_hash,user_id,expires_at) VALUES (?,?,?)",
            (_token_hash(token), row["id"], _iso(_now() + timedelta(minutes=15))),
        )
        _audit(connection, row["id"], "auth.password_reset_requested")
        return token


def consume_password_reset(token: str, password: str) -> tuple[bool, str]:
    error = validate_password(password)
    if error:
        return False, error
    with _db() as connection:
        row = connection.execute(
            "SELECT user_id, expires_at, used FROM password_reset_tokens WHERE token_hash=?",
            (_token_hash(token),),
        ).fetchone()
        if not row or row["used"] or datetime.fromisoformat(row["expires_at"]) <= _now():
            return False, "El enlace de recuperación no es válido o expiró."
        connection.execute("UPDATE users SET password_hash=?, failed_attempts=0, is_locked=0, updated_at=? WHERE id=?", (_hash_password(password), _iso(_now()), row["user_id"]))
        connection.execute("UPDATE password_reset_tokens SET used=1 WHERE token_hash=?", (_token_hash(token),))
        _audit(connection, row["user_id"], "auth.password_reset_completed")
        return True, "Contraseña restablecida."


def update_settings(token_ttl: int, session_idle: int, actor_id: int) -> None:
    with _db() as connection:
        connection.execute("UPDATE app_settings SET value=? WHERE name='token_ttl_minutes'", (str(token_ttl),))
        connection.execute("UPDATE app_settings SET value=? WHERE name='session_idle_minutes'", (str(session_idle),))
        _audit(connection, actor_id, "settings.session_policy_updated", details=f"token={token_ttl};idle={session_idle}")


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
