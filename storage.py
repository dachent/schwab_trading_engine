from __future__ import annotations

import base64
import ctypes
import json
import os
import re
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator
from urllib.parse import urlsplit, urlunsplit


class DATA_BLOB(ctypes.Structure):
    _fields_ = [("cbData", ctypes.c_uint32), ("pbData", ctypes.POINTER(ctypes.c_byte))]


kernel32 = ctypes.windll.kernel32
crypt32 = ctypes.windll.crypt32


def _blob_from_bytes(data: bytes) -> DATA_BLOB:
    if not data:
        return DATA_BLOB(0, None)
    buffer = ctypes.create_string_buffer(data, len(data))
    return DATA_BLOB(len(data), ctypes.cast(buffer, ctypes.POINTER(ctypes.c_byte)))


def _bytes_from_blob(blob: DATA_BLOB) -> bytes:
    if not blob.cbData:
        return b""
    pointer = ctypes.cast(blob.pbData, ctypes.POINTER(ctypes.c_ubyte))
    return bytes(pointer[i] for i in range(blob.cbData))


def dpapi_encrypt(plaintext: bytes) -> bytes:
    in_blob = _blob_from_bytes(plaintext)
    out_blob = DATA_BLOB()
    if not crypt32.CryptProtectData(ctypes.byref(in_blob), "schwab_tool", None, None, None, 0, ctypes.byref(out_blob)):
        raise ctypes.WinError()
    try:
        return _bytes_from_blob(out_blob)
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


def dpapi_decrypt(ciphertext: bytes) -> bytes:
    in_blob = _blob_from_bytes(ciphertext)
    out_blob = DATA_BLOB()
    if not crypt32.CryptUnprotectData(ctypes.byref(in_blob), None, None, None, None, 0, ctypes.byref(out_blob)):
        raise ctypes.WinError()
    try:
        return _bytes_from_blob(out_blob)
    finally:
        if out_blob.pbData:
            kernel32.LocalFree(out_blob.pbData)


@dataclass(frozen=True)
class AppPaths:
    root: Path
    jobs_dir: Path
    results_dir: Path
    logs_dir: Path
    exports_dir: Path
    imports_dir: Path
    state_dir: Path
    db_path: Path
    credentials_path: Path


def get_app_paths(root: Path | None = None) -> AppPaths:
    base = root or Path(__file__).resolve().parent
    return AppPaths(
        root=base,
        jobs_dir=base / "jobs",
        results_dir=base / "results",
        logs_dir=base / "logs",
        exports_dir=base / "exports",
        imports_dir=base / "imports",
        state_dir=base / "state",
        db_path=base / "state" / "app.db",
        credentials_path=base / "state" / "credentials.json.dpapi",
    )


def ensure_runtime_dirs(paths: AppPaths | None = None) -> AppPaths:
    runtime = paths or get_app_paths()
    for path in (
        runtime.root,
        runtime.jobs_dir,
        runtime.results_dir,
        runtime.logs_dir,
        runtime.exports_dir,
        runtime.imports_dir,
        runtime.state_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)
    return runtime


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(text, encoding="utf-8")
    os.replace(temp, path)


def atomic_write_json(path: Path, payload: Any) -> None:
    atomic_write_text(path, json.dumps(payload, indent=2, ensure_ascii=False))


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


class CredentialStore:
    def __init__(self, credentials_path: Path | None = None) -> None:
        self.paths = ensure_runtime_dirs()
        self.credentials_path = credentials_path or self.paths.credentials_path

    def load(self) -> dict[str, Any]:
        if not self.credentials_path.exists():
            return {}
        encrypted = base64.b64decode(self.credentials_path.read_text(encoding="utf-8"))
        return json.loads(dpapi_decrypt(encrypted).decode("utf-8"))

    def save(self, payload: dict[str, Any]) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        encrypted = dpapi_encrypt(encoded)
        atomic_write_text(self.credentials_path, base64.b64encode(encrypted).decode("ascii"))

    def merge(self, **updates: Any) -> dict[str, Any]:
        payload = self.load()
        payload.update({k: v for k, v in updates.items() if v is not None})
        self.save(payload)
        return payload


@contextmanager
def db_connection(db_path: Path | None = None) -> Iterator[sqlite3.Connection]:
    runtime = ensure_runtime_dirs()
    path = db_path or runtime.db_path
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def init_db(db_path: Path | None = None) -> None:
    with db_connection(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS order_runs (
                run_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL,
                import_path TEXT NOT NULL,
                execution_profile_json TEXT NOT NULL,
                summary_json TEXT
            );

            CREATE TABLE IF NOT EXISTS placed_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                request_id TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                account_number TEXT NOT NULL,
                account_hash TEXT,
                symbol TEXT NOT NULL,
                quantity REAL NOT NULL,
                order_id TEXT,
                http_status INTEGER,
                location TEXT,
                response_body TEXT,
                local_status TEXT NOT NULL,
                local_status_detail TEXT,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS broker_orders (
                order_id TEXT PRIMARY KEY,
                account_name TEXT,
                account_number TEXT NOT NULL,
                account_hash TEXT NOT NULL,
                quantity REAL,
                symbol TEXT,
                price REAL,
                entered_time TEXT,
                time_in_force TEXT,
                session TEXT,
                status TEXT,
                status_details TEXT,
                cost_basis_method TEXT,
                cancelable INTEGER NOT NULL DEFAULT 0,
                raw_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS account_snapshots (
                account_number TEXT PRIMARY KEY,
                account_name TEXT NOT NULL,
                account_hash TEXT NOT NULL,
                cash_available REAL,
                liquidation_value REAL,
                raw_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS position_snapshots (
                account_number TEXT NOT NULL,
                symbol TEXT NOT NULL,
                account_name TEXT NOT NULL,
                account_hash TEXT NOT NULL,
                average_price REAL,
                quantity REAL,
                value REAL,
                day_pl REAL,
                raw_json TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (account_number, symbol)
            );
            """
        )


def save_setting(key: str, value: Any, db_path: Path | None = None) -> None:
    init_db(db_path)
    with db_connection(db_path) as conn:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, json.dumps(value)),
        )


def load_settings(db_path: Path | None = None) -> dict[str, Any]:
    init_db(db_path)
    with db_connection(db_path) as conn:
        rows = conn.execute("SELECT key, value FROM settings").fetchall()
    return {row["key"]: json.loads(row["value"]) for row in rows}


_REDACTED = "[REDACTED]"
_SENSITIVE_FIELDS = {
    "app_secret",
    "token",
    "access_token",
    "refresh_token",
    "id_token",
    "authorization",
    "authorization_response",
    "client_secret",
}
_URL_FIELDS = {
    "callback_url",
    "received_url",
    "authorization_response",
    "redirect_url",
    "location",
}
_TEXT_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"(?i)([?&](?:code|access_token|refresh_token|id_token|token)=)([^&#\s]+)"),
        r"\1[REDACTED]",
    ),
    (
        re.compile(r"(?i)(\"(?:app_secret|access_token|refresh_token|id_token|client_secret)\"\s*:\s*\")([^\"]+)(\")"),
        r"\1[REDACTED]\3",
    ),
    (
        re.compile(r"(?i)('(?:app_secret|access_token|refresh_token|id_token|client_secret)'\s*:\s*')([^']+)(')"),
        r"\1[REDACTED]\3",
    ),
)


def _redact_url(value: str) -> str:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return _redact_text(value)
    if not (parsed.scheme and parsed.netloc):
        return _redact_text(value)
    query = "[REDACTED]" if parsed.query else ""
    fragment = "[REDACTED]" if parsed.fragment else ""
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, query, fragment))


def _redact_text(value: str) -> str:
    sanitized = value
    for pattern, replacement in _TEXT_REDACTIONS:
        sanitized = pattern.sub(replacement, sanitized)
    return sanitized


def redact_sensitive_data(value: Any, key: str | None = None) -> Any:
    lowered_key = (key or "").lower()
    if lowered_key in _SENSITIVE_FIELDS:
        return _REDACTED
    if isinstance(value, dict):
        return {child_key: redact_sensitive_data(child_value, child_key) for child_key, child_value in value.items()}
    if isinstance(value, list):
        return [redact_sensitive_data(item, key) for item in value]
    if isinstance(value, tuple):
        return [redact_sensitive_data(item, key) for item in value]
    if isinstance(value, str):
        if lowered_key in _URL_FIELDS:
            return _redact_url(value)
        return _redact_text(value)
    return value


def append_audit_record(record: dict[str, Any], audit_path: Path | None = None) -> None:
    runtime = ensure_runtime_dirs()
    path = audit_path or runtime.logs_dir / "audit.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(redact_sensitive_data(record), ensure_ascii=False) + "\n")


def sanitize_runtime_artifacts(paths: AppPaths | None = None) -> None:
    runtime = ensure_runtime_dirs(paths)
    for directory in (runtime.jobs_dir, runtime.results_dir):
        for artifact_path in directory.glob("*.json"):
            payload = read_json(artifact_path)
            if payload is None:
                continue
            atomic_write_json(artifact_path, redact_sensitive_data(payload))

    audit_path = runtime.logs_dir / "audit.jsonl"
    if not audit_path.exists():
        return

    sanitized_lines: list[str] = []
    for raw_line in audit_path.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip():
            continue
        try:
            payload = json.loads(raw_line)
        except json.JSONDecodeError:
            sanitized_lines.append(_redact_text(raw_line))
        else:
            sanitized_lines.append(json.dumps(redact_sensitive_data(payload), ensure_ascii=False))
    atomic_write_text(audit_path, "\n".join(sanitized_lines) + ("\n" if sanitized_lines else ""))
