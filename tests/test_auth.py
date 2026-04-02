import json
import shutil
import tkinter as tk
import uuid
from pathlib import Path

import pytest
from schwab import auth

import tasks
import ui
from schwab_client import DEFAULT_CALLBACK_URL, SchwabClient, SchwabClientError
from storage import AppPaths, redact_sensitive_data


def _temp_paths(root: Path) -> AppPaths:
    return AppPaths(
        root=root,
        jobs_dir=root / "jobs",
        results_dir=root / "results",
        logs_dir=root / "logs",
        exports_dir=root / "exports",
        imports_dir=root / "imports",
        state_dir=root / "state",
        db_path=root / "state" / "app.db",
        credentials_path=root / "state" / "credentials.json.dpapi",
    )


def _temp_dir() -> Path:
    root = Path(__file__).resolve().parent / ".tmp"
    path = root / uuid.uuid4().hex
    path.mkdir(parents=True, exist_ok=False)
    return path


def test_validate_callback_url_accepts_explicit_loopback_urls() -> None:
    SchwabClient._validate_callback_url(DEFAULT_CALLBACK_URL)
    SchwabClient._validate_callback_url("https://127.0.0.1:9443/")

    with pytest.raises(SchwabClientError):
        SchwabClient._validate_callback_url("https://127.0.0.1/")

    with pytest.raises(SchwabClientError):
        SchwabClient._validate_callback_url("https://localhost:8182/")


def test_auth_error_mapping_is_actionable() -> None:
    timeout_message = SchwabClient._auth_error_message(auth.RedirectTimeoutError("timed out"), DEFAULT_CALLBACK_URL)
    bind_message = SchwabClient._auth_error_message(auth.RedirectServerExitedError("server exited"), DEFAULT_CALLBACK_URL)

    assert DEFAULT_CALLBACK_URL in timeout_message
    assert "approve the local certificate warning" in timeout_message.lower()
    assert "port 8182" in bind_message


def test_task_login_uses_non_interactive_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class StubClient:
        def save_credentials(self, app_key: str, app_secret: str, callback_url: str) -> None:
            calls["saved"] = (app_key, app_secret, callback_url)

        def load_credentials(self) -> dict[str, str]:
            return {"app_key": "KEY1234"}

        def connect(self, *, force_login: bool, interactive: bool, callback_timeout: float):
            calls["connect"] = {
                "force_login": force_login,
                "interactive": interactive,
                "callback_timeout": callback_timeout,
            }
            return object()

        def get_account_numbers(self) -> list[dict[str, str]]:
            return [{"accountNumber": "1234"}]

        def login_status(self) -> dict[str, bool]:
            return {"has_token": True}

    monkeypatch.setattr(tasks, "SchwabClient", StubClient)

    result = tasks.task_login(
        {
            "app_key": "KEY1234",
            "app_secret": "SECRET5678",
            "callback_url": DEFAULT_CALLBACK_URL,
            "force_login": True,
        }
    )

    assert calls["saved"] == ("KEY1234", "SECRET5678", DEFAULT_CALLBACK_URL)
    assert calls["connect"] == {
        "force_login": True,
        "interactive": False,
        "callback_timeout": 300.0,
    }
    assert result["linked_account_count"] == 1


def test_redact_sensitive_data_masks_auth_material() -> None:
    record = {
        "request": {
            "args": {
                "app_secret": "very-secret",
                "callback_url": "https://127.0.0.1:8182/?code=abc123",
            }
        },
        "result": {
            "output": {"token": {"access_token": "token-value"}},
            "error": {"traceback": "received_url=https://127.0.0.1:8182/?code=abc123&access_token=xyz"},
        },
    }

    sanitized = redact_sensitive_data(record)

    assert sanitized["request"]["args"]["app_secret"] == "[REDACTED]"
    assert sanitized["result"]["output"]["token"] == "[REDACTED]"
    assert sanitized["request"]["args"]["callback_url"] == "https://127.0.0.1:8182/?[REDACTED]"
    assert "abc123" not in sanitized["result"]["error"]["traceback"]
    assert "xyz" not in sanitized["result"]["error"]["traceback"]


def test_ui_login_writes_credentials_into_login_job(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_root = _temp_dir()
    paths = _temp_paths(temp_root)
    monkeypatch.setattr(ui, "get_app_paths", lambda: paths)
    monkeypatch.setattr(ui, "load_settings", lambda: {})
    monkeypatch.setattr(ui, "save_setting", lambda key, value: None)
    monkeypatch.setattr(ui.messagebox, "showwarning", lambda *args, **kwargs: None)
    monkeypatch.setattr(ui.messagebox, "showerror", lambda *args, **kwargs: None)
    monkeypatch.setattr(ui.tk.Misc, "after", lambda self, delay, callback=None, *args: None)

    class FakeThread:
        def __init__(self, target, daemon: bool) -> None:
            self.target = target
            self.daemon = daemon

        def start(self) -> None:
            return None

    monkeypatch.setattr(ui.threading, "Thread", FakeThread)

    root = tk.Tk()
    root.withdraw()
    try:
        app = ui.SchwabToolApp(root)
        app.app_key_var.set("KEY1234")
        app.app_secret_var.set("SECRET5678")
        app.callback_url_var.set("https://127.0.0.1:9443/")

        app._login()

        request_files = list(paths.jobs_dir.glob("*.json"))
        assert len(request_files) == 1

        request_payload = json.loads(request_files[0].read_text(encoding="utf-8"))
        assert request_payload["task_name"] == "login"
        assert request_payload["args"] == {
            "app_key": "KEY1234",
            "app_secret": "SECRET5678",
            "callback_url": "https://127.0.0.1:9443/",
            "force_login": True,
        }
        assert app.last_action_var.get() == "Opening browser for Schwab login"
        assert app.refresh_var.get() == "Waiting for redirect on https://127.0.0.1:9443/"
    finally:
        root.destroy()
        shutil.rmtree(temp_root, ignore_errors=True)
