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

import auth_diagnostic
import tasks
import ui
from schwab_client import (
    CALLBACK_PREFLIGHT_TIMEOUT_SECONDS,
    DEFAULT_CALLBACK_URL,
    CallbackRequest,
    SchwabClient,
    SchwabClientError,
)
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


def _create_tk_root() -> tk.Tk:
    try:
        root = tk.Tk()
    except tk.TclError as exc:
        pytest.skip(f"Tk is unavailable in this test environment: {exc}")
    root.withdraw()
    return root


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


def test_begin_manual_login_returns_authorization_url_and_redacted_diagnostics() -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)

        login = client.begin_manual_login()
        authorize_query = parse_qs(urlparse(login["diagnostics"]["authorize_url"]).query)

        assert login["authorization_url"].startswith("https://api.schwabapi.com/v1/oauth/authorize?")
        assert login["diagnostics"]["authorize_redirect_uri"] == callback_url
        assert login["diagnostics"]["callback_url"] == callback_url
        assert authorize_query["state"] == ["[REDACTED]"]
        assert client._pending_auth_context is not None
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_complete_manual_login_persists_token_and_verifies_accounts(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    verified_accounts = [{"accountNumber": "1234"}]
    captured: dict[str, object] = {}
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)
        client.begin_manual_login()

        def fake_client_from_received_url(
            api_key: str,
            app_secret: str,
            auth_context,
            received_url: str,
            token_write_func,
            *,
            enforce_enums: bool,
        ) -> object:
            captured["api_key"] = api_key
            captured["app_secret"] = app_secret
            captured["received_url"] = received_url
            captured["enforce_enums"] = enforce_enums
            token_write_func({"creation_timestamp": 123, "token": {"access_token": "token-value"}})
            return object()

        monkeypatch.setattr(auth, "client_from_received_url", fake_client_from_received_url)
        monkeypatch.setattr(client, "get_account_numbers", lambda: verified_accounts)

        linked = client.complete_manual_login(f"{callback_url}?code=abc123&state=oauth-state")
        stored = client.load_credentials()

        assert linked == verified_accounts
        assert client.last_verified_accounts() == verified_accounts
        assert stored["token"]["token"]["access_token"] == "token-value"
        assert captured["api_key"] == "KEY1234"
        assert captured["app_secret"] == "SECRET5678"
        assert captured["received_url"] == f"{callback_url}?code=abc123&state=oauth-state"
        assert captured["enforce_enums"] is False
        assert client._pending_auth_context is None
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_connect_reuses_existing_token_and_skips_browser_login(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    try:
        client = SchwabClient(root=temp_root)
        client.store.save(
            {
                "app_key": "KEY1234",
                "app_secret": "SECRET5678",
                "callback_url": callback_url,
                "token": {"creation_timestamp": 123, "token": {"access_token": "token-value"}},
            }
        )
        session = object()
        calls: dict[str, object] = {}

        def fake_existing_token_connect(_creds) -> object:
            client.client = session
            calls["existing"] = True
            return session

        def fake_verify() -> list[dict[str, str]]:
            calls["verified"] = True
            client._last_verified_accounts = [{"accountNumber": "1234"}]
            return client._last_verified_accounts

        monkeypatch.setattr(client, "_connect_from_existing_token", fake_existing_token_connect)
        monkeypatch.setattr(client, "_connect_via_browser_callback", lambda *args, **kwargs: pytest.fail("browser callback flow should not run"))
        monkeypatch.setattr(client, "_verify_login_session", fake_verify)

        returned = client.connect(force_login=False, interactive=False)

        assert returned is session
        assert calls == {"existing": True, "verified": True}
        assert client.last_verified_accounts() == [{"accountNumber": "1234"}]
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_connect_uses_browser_callback_flow_for_forced_login(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    calls: dict[str, object] = {}
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)

        def fake_browser_callback(_creds, interactive: bool, callback_timeout: float, requested_browser: str | None = None):
            calls["browser_callback"] = {
                "interactive": interactive,
                "callback_timeout": callback_timeout,
                "requested_browser": requested_browser,
            }
            client.client = object()
            client._last_verified_accounts = [{"accountNumber": "1234"}]
            return client.client

        monkeypatch.setattr(client, "_connect_via_browser_callback", fake_browser_callback)

        returned = client.connect(force_login=True, interactive=False, callback_timeout=12.5, requested_browser="chrome")

        assert returned is client.client
        assert calls["browser_callback"] == {
            "interactive": False,
            "callback_timeout": 12.5,
            "requested_browser": "chrome",
        }
        assert client.last_verified_accounts() == [{"accountNumber": "1234"}]
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_connect_via_browser_callback_routes_received_url_through_shared_completion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    opened: list[str] = []
    calls: dict[str, object] = {}
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)
        creds = client._load_credentials()

        @contextmanager
        def fake_listener(url: str):
            assert url == callback_url
            yield object()

        callback_requests = [
            CallbackRequest(url=callback_url, request_path="/", matched_callback=True),
            CallbackRequest(
                url=f"{callback_url}?code=abc123&state=oauth-state",
                request_path="/",
                matched_callback=True,
            ),
        ]

        def fake_wait(_queue, timeout_seconds: float) -> CallbackRequest:
            calls.setdefault("timeouts", []).append(timeout_seconds)
            return callback_requests.pop(0)

        def fake_browser_opener(requested_browser: str | None):
            calls["requested_browser"] = requested_browser
            return lambda url: opened.append(url) or True

        def fake_consume(_creds, received_url: str, *, auth_context=None) -> list[dict[str, str]]:
            calls["received_url"] = received_url
            calls["auth_context"] = auth_context
            client.client = object()
            client._last_verified_accounts = [{"accountNumber": "1234"}]
            return client._last_verified_accounts

        monkeypatch.setattr(client, "_callback_listener_server", fake_listener)
        monkeypatch.setattr(client, "_wait_for_callback_request", fake_wait)
        monkeypatch.setattr(client, "_consume_received_url", fake_consume)
        monkeypatch.setattr(SchwabClient, "_browser_opener", staticmethod(fake_browser_opener))

        returned = client._connect_via_browser_callback(
            creds,
            interactive=False,
            callback_timeout=15.0,
            requested_browser="firefox",
        )

        assert returned is client.client
        assert calls["requested_browser"] == "firefox"
        assert calls["timeouts"] == [CALLBACK_PREFLIGHT_TIMEOUT_SECONDS, 15.0]
        assert opened[0] == callback_url
        assert opened[1].startswith("https://api.schwabapi.com/v1/oauth/authorize?")
        assert calls["received_url"] == f"{callback_url}?code=abc123&state=oauth-state"
        assert calls["auth_context"] is not None
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_connect_reports_timeout_after_preflight_success(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)

        @contextmanager
        def fake_listener(url: str):
            assert url == callback_url
            yield object()

        callback_requests = [CallbackRequest(url=callback_url, request_path="/", matched_callback=True)]

        def fake_wait(_queue, _timeout_seconds: float) -> CallbackRequest:
            if callback_requests:
                return callback_requests.pop(0)
            raise auth.RedirectTimeoutError("timed out")

        monkeypatch.setattr(client, "_callback_listener_server", fake_listener)
        monkeypatch.setattr(client, "_wait_for_callback_request", fake_wait)
        monkeypatch.setattr(SchwabClient, "_browser_opener", staticmethod(lambda requested_browser=None: (lambda url: True)))

        with pytest.raises(SchwabClientError) as exc_info:
            client.connect(force_login=True, interactive=False, callback_timeout=5.0)

        assert "local callback preflight succeeded" in str(exc_info.value).lower()
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_connect_reports_unexpected_callback_path(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)

        @contextmanager
        def fake_listener(url: str):
            assert url == callback_url
            yield object()

        callback_requests = [
            CallbackRequest(url=callback_url, request_path="/", matched_callback=True),
            CallbackRequest(url=f"https://127.0.0.1:{urlparse(callback_url).port}/wrong-path?code=abc123", request_path="/wrong-path", matched_callback=False),
        ]

        def fake_wait(_queue, _timeout_seconds: float) -> CallbackRequest:
            return callback_requests.pop(0)

        monkeypatch.setattr(client, "_callback_listener_server", fake_listener)
        monkeypatch.setattr(client, "_wait_for_callback_request", fake_wait)
        monkeypatch.setattr(SchwabClient, "_browser_opener", staticmethod(lambda requested_browser=None: (lambda url: True)))

        with pytest.raises(SchwabClientError) as exc_info:
            client.connect(force_login=True, interactive=False, callback_timeout=5.0)

        assert "/wrong-path" in str(exc_info.value)
        assert callback_url in str(exc_info.value)
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_task_login_uses_non_interactive_connect(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: dict[str, object] = {}

    class StubClient:
        def save_credentials(self, app_key: str, app_secret: str, callback_url: str) -> None:
            calls["saved"] = (app_key, app_secret, callback_url)

        def load_credentials(self) -> dict[str, str]:
            return {"app_key": "KEY1234"}

        def connect(self, *, force_login: bool, interactive: bool, callback_timeout: float, requested_browser: str | None = None):
            calls["connect"] = {
                "force_login": force_login,
                "interactive": interactive,
                "callback_timeout": callback_timeout,
                "requested_browser": requested_browser,
            }
            return object()

        def last_verified_accounts(self) -> list[dict[str, str]]:
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
            "requested_browser": "chrome",
        }
    )

    assert calls["saved"] == ("KEY1234", "SECRET5678", DEFAULT_CALLBACK_URL)
    assert calls["connect"] == {
        "force_login": True,
        "interactive": False,
        "callback_timeout": 300.0,
        "requested_browser": "chrome",
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

    root = _create_tk_root()
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

    root = _create_tk_root()
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
        monkeypatch.setattr(client, "_wait_for_received_url", lambda queue, timeout_seconds, callback_url=None: callback_url)

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

        def fake_wait(queue, timeout_seconds: float, callback_url: str | None = None):
            raise auth.RedirectTimeoutError("timed out")

        monkeypatch.setattr(client, "_callback_preflight_server", fake_server)
        monkeypatch.setattr(client, "_wait_for_received_url", fake_wait)

        with pytest.raises(SchwabClientError) as exc_info:
            client._run_callback_preflight(callback_url, timeout_seconds=1.0, open_browser=lambda url: True)

        assert "local callback preflight failed" in str(exc_info.value).lower()
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_callback_preflight_reports_browser_open_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    temp_root = _temp_dir()
    try:
        client = SchwabClient(root=temp_root)
        callback_url = f"https://127.0.0.1:{_free_port()}"
        queue_marker = object()

        @contextmanager
        def fake_server(url: str):
            assert url == callback_url
            yield queue_marker

        monkeypatch.setattr(client, "_callback_preflight_server", fake_server)

        with pytest.raises(SchwabClientError) as exc_info:
            client._run_callback_preflight(callback_url, timeout_seconds=1.0, open_browser=lambda url: False)

        assert "unable to open the default browser" in str(exc_info.value).lower()
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_complete_manual_login_requires_pending_auth_context() -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)

        with pytest.raises(SchwabClientError) as exc_info:
            client.complete_manual_login(f"{callback_url}?code=abc123&state=oauth-state")

        assert "authorization context was missing or reset" in str(exc_info.value).lower()
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_complete_manual_login_reports_token_exchange_failure_after_callback_received(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    temp_root = _temp_dir()
    callback_url = f"https://127.0.0.1:{_free_port()}"
    try:
        client = SchwabClient(root=temp_root)
        client.save_credentials("KEY1234", "SECRET5678", callback_url)
        client.begin_manual_login()

        def fake_client_from_received_url(*args, **kwargs):
            raise ValueError("invalid_grant")

        monkeypatch.setattr(auth, "client_from_received_url", fake_client_from_received_url)

        with pytest.raises(SchwabClientError) as exc_info:
            client.complete_manual_login(f"{callback_url}?code=abc123&state=oauth-state")

        message = str(exc_info.value).lower()
        assert "reached the local callback url" in message
        assert "rejected the oauth code exchange" in message
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def test_auth_diagnostic_manual_mode(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    calls: dict[str, object] = {}

    class StubClient:
        def save_credentials(self, app_key: str, app_secret: str, callback_url: str) -> None:
            calls["saved"] = (app_key, app_secret, callback_url)

        def clear_token(self) -> None:
            calls["cleared"] = True

        def begin_manual_login(self) -> dict[str, object]:
            return {
                "authorization_url": "https://auth.example/authorize",
                "diagnostics": {
                    "callback_url": DEFAULT_CALLBACK_URL,
                    "callback_port": 8182,
                    "callback_path": "<empty>",
                    "authorize_redirect_uri": DEFAULT_CALLBACK_URL,
                    "authorize_url": "https://auth.example/authorize?state=[REDACTED]",
                },
            }

        def complete_manual_login(self, received_url: str) -> list[dict[str, str]]:
            calls["received_url"] = received_url
            return [{"accountNumber": "1234"}]

        def login_status(self) -> dict[str, object]:
            return {"callback_url": DEFAULT_CALLBACK_URL, "has_token": True}

    monkeypatch.setattr(auth_diagnostic, "SchwabClient", StubClient)
    monkeypatch.setattr("builtins.input", lambda prompt="": f"{DEFAULT_CALLBACK_URL}?code=abc123")

    exit_code = auth_diagnostic.main(
        [
            "manual",
            "--app-key",
            "KEY1234",
            "--app-secret",
            "SECRET5678",
            "--callback-url",
            DEFAULT_CALLBACK_URL,
            "--force-refresh",
        ]
    )

    output = capsys.readouterr().out
    assert exit_code == 0
    assert calls["saved"] == ("KEY1234", "SECRET5678", DEFAULT_CALLBACK_URL)
    assert calls["cleared"] is True
    assert calls["received_url"] == f"{DEFAULT_CALLBACK_URL}?code=abc123"
    assert "https://auth.example/authorize" in output
    assert "linked_accounts: 1" in output


def test_auth_diagnostic_auto_mode_passes_browser_override(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, object] = {}

    class StubClient:
        def connect(
            self,
            *,
            force_login: bool,
            interactive: bool,
            callback_timeout: float,
            requested_browser: str | None = None,
        ) -> object:
            calls["connect"] = {
                "force_login": force_login,
                "interactive": interactive,
                "callback_timeout": callback_timeout,
                "requested_browser": requested_browser,
            }
            return object()

        def last_verified_accounts(self) -> list[dict[str, str]]:
            return [{"accountNumber": "1234"}]

        def login_status(self) -> dict[str, object]:
            return {"callback_url": DEFAULT_CALLBACK_URL, "has_token": True}

    monkeypatch.setattr(auth_diagnostic, "SchwabClient", StubClient)

    exit_code = auth_diagnostic.main(["auto", "--timeout", "42", "--browser", "firefox"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert calls["connect"] == {
        "force_login": True,
        "interactive": False,
        "callback_timeout": 42.0,
        "requested_browser": "firefox",
    }
    assert "linked_accounts: 1" in output
