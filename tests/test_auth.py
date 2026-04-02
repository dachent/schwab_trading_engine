import json
import shutil
import socket
import tkinter as tk
import uuid
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlparse

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


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as handle:
        handle.bind(("127.0.0.1", 0))
        return int(handle.getsockname()[1])


def test_validate_callback_url_accepts_explicit_loopback_urls() -> None:
    SchwabClient._validate_callback_url(DEFAULT_CALLBACK_URL)
    SchwabClient._validate_callback_url("https://127.0.0.1:9443")

    with pytest.raises(SchwabClientError):
        SchwabClient._validate_callback_url("https://127.0.0.1/")

    with pytest.raises(SchwabClientError):
        SchwabClient._validate_callback_url("https://localhost:8182")


def test_authorization_diagnostics_use_exact_callback_value() -> None:
    diagnostics = SchwabClient._authorization_diagnostics("KEY1234", "https://127.0.0.1:9443")
    parsed = urlparse(diagnostics["authorize_url"])
    query = parse_qs(parsed.query)

    assert parsed.scheme == "https"
    assert parsed.netloc == "api.schwabapi.com"
    assert parsed.path == "/v1/oauth/authorize"
    assert query["client_id"] == ["...1234"]
    assert query["redirect_uri"] == ["https://127.0.0.1:9443"]
    assert query["state"] == ["[REDACTED]"]
    assert diagnostics["authorize_redirect_uri"] == "https://127.0.0.1:9443"


def test_auth_error_mapping_is_actionable() -> None:
    temp_root = _temp_dir()
    try:
        client = SchwabClient(root=temp_root)
        client._last_preflight_succeeded = True

        timeout_message = client._auth_error_message(auth.RedirectTimeoutError("timed out"), DEFAULT_CALLBACK_URL)
        bind_message = client._auth_error_message(auth.RedirectServerExitedError("server exited"), DEFAULT_CALLBACK_URL)

        assert DEFAULT_CALLBACK_URL in timeout_message
        assert "listener is reachable" in timeout_message.lower()
        assert "port 8182" in bind_message
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


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


def test_ui_login_persists_credentials_but_writes_minimal_job_payload(
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
        app.callback_url_var.set("https://127.0.0.1:9443")

        app._login()

        request_files = list(paths.jobs_dir.glob("*.json"))
        assert len(request_files) == 1

        request_payload = json.loads(request_files[0].read_text(encoding="utf-8"))
        assert request_payload["task_name"] == "login"
        assert request_payload["args"] == {"use_stored_credentials": True, "force_login": True}
        assert "SECRET5678" not in request_files[0].read_text(encoding="utf-8")
        assert app.last_action_var.get() == "Opening browser for Schwab login"
        assert app.refresh_var.get() == "Waiting for redirect on https://127.0.0.1:9443"
        assert "certificate warning" in app.login_status_var.get().lower()
    finally:
        root.destroy()
        shutil.rmtree(temp_root, ignore_errors=True)


def test_ui_login_rejects_trailing_slash_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_root = _temp_dir()
    paths = _temp_paths(temp_root)
    errors: list[tuple[tuple[object, ...], dict[str, object]]] = []

    monkeypatch.setattr(ui, "get_app_paths", lambda: paths)
    monkeypatch.setattr(ui, "load_settings", lambda: {})
    monkeypatch.setattr(ui, "save_setting", lambda key, value: None)
    monkeypatch.setattr(ui.messagebox, "showwarning", lambda *args, **kwargs: None)
    monkeypatch.setattr(ui.messagebox, "showerror", lambda *args, **kwargs: errors.append((args, kwargs)))
    monkeypatch.setattr(ui.tk.Misc, "after", lambda self, delay, callback=None, *args: None)

    root = tk.Tk()
    root.withdraw()
    try:
        app = ui.SchwabToolApp(root)
        app.app_key_var.set("KEY1234")
        app.app_secret_var.set("SECRET5678")
        app.callback_url_var.set("https://127.0.0.1:9443/")

        app._login()

        assert errors
        assert "without trailing slashes" in str(errors[0][0][1]).lower()
        assert not list(paths.jobs_dir.glob("*.json"))
    finally:
        root.destroy()
        shutil.rmtree(temp_root, ignore_errors=True)


def test_callback_preflight_success(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    try:
        client = SchwabClient(root=temp_root)
        callback_url = f"https://127.0.0.1:{_free_port()}"
        queue_marker = object()

        @contextmanager
        def fake_server(url: str):
            assert url == callback_url
            yield queue_marker

        opened: list[str] = []
        monkeypatch.setattr(client, "_callback_preflight_server", fake_server)
        monkeypatch.setattr(client, "_wait_for_received_url", lambda queue, timeout_seconds: callback_url)

        def open_browser(url: str) -> bool:
            opened.append(url)
            return True

        client._run_callback_preflight(callback_url, timeout_seconds=3.0, open_browser=open_browser)
        assert opened == [callback_url]
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_callback_preflight_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    try:
        client = SchwabClient(root=temp_root)
        callback_url = f"https://127.0.0.1:{_free_port()}"
        queue_marker = object()

        @contextmanager
        def fake_server(url: str):
            assert url == callback_url
            yield queue_marker

        def fake_wait(queue, timeout_seconds: float):
            raise auth.RedirectTimeoutError("timed out")

        monkeypatch.setattr(client, "_callback_preflight_server", fake_server)
        monkeypatch.setattr(client, "_wait_for_received_url", fake_wait)

        with pytest.raises(SchwabClientError) as exc_info:
            client._run_callback_preflight(callback_url, timeout_seconds=1.0, open_browser=lambda url: True)

        assert "local callback preflight failed" in str(exc_info.value).lower()
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
