from __future__ import annotations

import json
import logging
import queue
import threading
import time
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
from schwab import auth

from order_builders import normalize_tax_lot_method
from schemas import AccountSnapshot, BrokerOrder, PositionSnapshot, QuoteData
from storage import CredentialStore, ensure_runtime_dirs, get_app_paths, redact_sensitive_data


BASE_URL = "https://api.schwabapi.com"
DEFAULT_CALLBACK_URL = "https://127.0.0.1:8182"
DEFAULT_CALLBACK_TIMEOUT_SECONDS = 300.0
LOGIN_TASK_TIMEOUT_SECONDS = int(DEFAULT_CALLBACK_TIMEOUT_SECONDS + 45)
CALLBACK_PREFLIGHT_TIMEOUT_SECONDS = 30.0
CALLBACK_PREFLIGHT_STATUS_PATH = "/schwab-tool-auth/status"
IGNORED_CALLBACK_PATHS = frozenset({"/favicon.ico"})


logger = logging.getLogger("runner")


class SchwabClientError(RuntimeError):
    pass


@dataclass
class Credentials:
    app_key: str
    app_secret: str
    callback_url: str


@dataclass(frozen=True)
class CallbackRequest:
    url: str
    request_path: str
    matched_callback: bool


class SchwabClient:
    def __init__(self, root: Path | None = None) -> None:
        self.paths = ensure_runtime_dirs(get_app_paths(root))
        self.store = CredentialStore(self.paths.credentials_path)
        self.client = None
        self._last_preflight_succeeded: bool | None = None
        self._pending_auth_context: Any | None = None
        self._last_verified_accounts: list[dict[str, Any]] = []

    def load_credentials(self) -> dict[str, Any]:
        return self.store.load()

    def save_credentials(self, app_key: str, app_secret: str, callback_url: str) -> dict[str, Any]:
        callback_url = callback_url.strip()
        self._validate_callback_url(callback_url)
        current = self.store.load()
        token = current.get("token")
        payload = {
            "app_key": app_key.strip(),
            "app_secret": app_secret.strip(),
            "callback_url": callback_url,
        }
        if token:
            payload["token"] = token
        self.store.save(payload)
        return payload

    def clear_token(self) -> None:
        payload = self.store.load()
        payload.pop("token", None)
        self.store.save(payload)

    def _load_credentials(self) -> Credentials:
        payload = self.store.load()
        app_key = str(payload.get("app_key", "")).strip()
        app_secret = str(payload.get("app_secret", "")).strip()
        callback_url = str(payload.get("callback_url", "")).strip()
        if not app_key or not app_secret or not callback_url:
            raise SchwabClientError("App key, app secret, and callback URL must be configured first.")
        self._validate_callback_url(callback_url)
        return Credentials(app_key=app_key, app_secret=app_secret, callback_url=callback_url)

    @staticmethod
    def _validate_callback_url(callback_url: str) -> None:
        normalized = callback_url.strip()
        parsed = urlparse(normalized)
        if parsed.scheme != "https":
            raise SchwabClientError("Callback URL must use https.")
        if parsed.hostname != "127.0.0.1":
            raise SchwabClientError("Callback URL must use host 127.0.0.1.")
        if parsed.port is None:
            raise SchwabClientError("Callback URL must include an explicit loopback port, for example https://127.0.0.1:8182.")
        if parsed.path and not parsed.path.startswith("/"):
            raise SchwabClientError("Callback URL path must start with '/'.")
        if parsed.query or parsed.fragment:
            raise SchwabClientError("Callback URL cannot contain query parameters or fragments.")

    @staticmethod
    def _normalize_callback_path(path: str) -> str:
        return path or "/"

    @staticmethod
    def _display_path(path: str) -> str:
        return path or "<empty>"

    @classmethod
    def _listener_status_path(cls, callback_path: str) -> str:
        status_path = CALLBACK_PREFLIGHT_STATUS_PATH
        while status_path == callback_path:
            status_path = f"{status_path}/ready"
        return status_path

    @classmethod
    def _callback_components(cls, callback_url: str) -> tuple[int, str, str]:
        parsed = urlparse(callback_url)
        if parsed.port is None:
            raise SchwabClientError("Callback URL must include an explicit loopback port.")
        return (
            parsed.port,
            cls._normalize_callback_path(parsed.path),
            cls._display_path(parsed.path),
        )

    @classmethod
    def _validate_received_callback_url(cls, callback_url: str, received_url: str) -> None:
        expected = urlparse(callback_url)
        received = urlparse(received_url)
        expected_path = cls._normalize_callback_path(expected.path)
        received_path = cls._normalize_callback_path(received.path)
        received_host = received.hostname or "<unknown>"
        received_port = received.port or "<unknown>"

        if received.scheme != expected.scheme or received.hostname != expected.hostname or received.port != expected.port:
            raise SchwabClientError(
                f"Received redirect URL targeted {received.scheme}://{received_host}:{received_port} instead of {callback_url}. "
                "Confirm the Schwab developer app callback URL exactly matches the configured loopback URL."
            )
        if received_path != expected_path:
            raise SchwabClientError(
                f"Received redirect URL used unexpected path {received_path}. Expected {expected_path} from {callback_url}. "
                "Confirm the Schwab developer app callback URL exactly matches the configured loopback URL."
            )

    def _wait_for_https_server(
        self,
        *,
        status_url: str,
        timeout_seconds: float,
        server_thread: threading.Thread,
        startup_errors: queue.Queue[BaseException],
    ) -> None:
        deadline = time.time() + max(5.0, min(timeout_seconds, 15.0))
        while time.time() < deadline:
            try:
                startup_error = startup_errors.get_nowait()
            except queue.Empty:
                startup_error = None
            if startup_error is not None:
                raise auth.RedirectServerExitedError("Local callback listener exited before it started.") from startup_error
            if not server_thread.is_alive():
                raise auth.RedirectServerExitedError("Local callback listener exited before it started.")
            try:
                response = httpx.get(status_url, verify=False, timeout=1.0)
            except httpx.HTTPError:
                time.sleep(0.1)
                continue
            if response.status_code == 200:
                return
            time.sleep(0.1)
        raise auth.RedirectServerExitedError("Local callback listener did not start in time.")

    @contextmanager
    def _callback_listener_server(self, callback_url: str) -> Any:
        import flask
        from werkzeug.serving import make_server

        callback_port, callback_path, _ = self._callback_components(callback_url)
        status_path = self._listener_status_path(callback_path)
        callback_queue: queue.Queue[CallbackRequest] = queue.Queue()
        startup_errors: queue.Queue[BaseException] = queue.Queue()
        server_holder: dict[str, Any] = {}

        def serve() -> None:
            app = flask.Flask(__name__)
            werkzeug_logger = logging.getLogger("werkzeug")
            previous_level = werkzeug_logger.level
            werkzeug_logger.setLevel(logging.ERROR)
            try:

                @app.route(status_path)
                def status() -> str:
                    return "running"

                @app.route("/", defaults={"requested_path": ""})
                @app.route("/<path:requested_path>")
                def handle_callback(requested_path: str) -> Any:
                    request_path = self._normalize_callback_path(flask.request.path)
                    if request_path == status_path:
                        return "running"
                    if request_path in IGNORED_CALLBACK_PATHS:
                        return flask.Response(status=204)
                    callback_queue.put(
                        CallbackRequest(
                            url=flask.request.url,
                            request_path=request_path,
                            matched_callback=request_path == callback_path,
                        )
                    )
                    if request_path == callback_path:
                        if flask.request.args.get("code"):
                            return "Schwab authorization callback received. You may now return to the app."
                        return "Schwab local callback probe received. You may now close this window/tab."
                    return flask.Response(
                        "Unexpected callback path. Confirm the configured callback URL exactly matches the Schwab developer app entry.",
                        status=404,
                    )

                server = make_server("127.0.0.1", callback_port, app, ssl_context="adhoc", threaded=True)
                server_holder["server"] = server
                server.serve_forever()
            except BaseException as exc:  # noqa: BLE001
                startup_errors.put(exc)
            finally:
                server = server_holder.get("server")
                if server is not None:
                    try:
                        server.server_close()
                    except Exception:  # noqa: BLE001
                        pass
                werkzeug_logger.setLevel(previous_level)

        server_thread = threading.Thread(target=serve, daemon=True)
        server_thread.start()
        try:
            self._wait_for_https_server(
                status_url=f"https://127.0.0.1:{callback_port}{status_path}",
                timeout_seconds=CALLBACK_PREFLIGHT_TIMEOUT_SECONDS,
                server_thread=server_thread,
                startup_errors=startup_errors,
            )
            yield callback_queue
        finally:
            server = server_holder.get("server")
            if server is not None:
                server.shutdown()
            server_thread.join(timeout=5)

    def _callback_preflight_server(self, callback_url: str) -> Any:
        return self._callback_listener_server(callback_url)

    def _wait_for_callback_request(self, callback_queue: queue.Queue[CallbackRequest], timeout_seconds: float) -> CallbackRequest:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            remaining = max(0.1, deadline - time.time())
            try:
                callback_request = callback_queue.get(timeout=min(remaining, 0.25))
            except queue.Empty:
                continue
            if callback_request.request_path in IGNORED_CALLBACK_PATHS:
                continue
            return callback_request
        raise auth.RedirectTimeoutError(
            "Timed out waiting for a post-authorization callback. You can set a longer timeout by passing a value of callback_timeout."
        )

    def _wait_for_received_url(
        self,
        callback_queue: queue.Queue[CallbackRequest],
        timeout_seconds: float,
        *,
        callback_url: str,
    ) -> str:
        callback_request = self._wait_for_callback_request(callback_queue, timeout_seconds)
        if not callback_request.matched_callback:
            raise SchwabClientError(self._callback_path_mismatch_message(callback_url, callback_request.request_path))
        return callback_request.url

    def _connect_from_existing_token(self, creds: Credentials):
        self.client = auth.client_from_access_functions(
            creds.app_key,
            creds.app_secret,
            self._token_read,
            self._token_write,
            enforce_enums=False,
        )
        return self.client

    @staticmethod
    def _build_auth_context(creds: Credentials):
        return auth.get_auth_context(creds.app_key, creds.callback_url)

    @staticmethod
    def _authorization_diagnostics(app_key: str, callback_url: str, auth_context: Any | None = None) -> dict[str, Any]:
        parsed_callback = urlparse(callback_url)
        auth_context = auth_context or auth.get_auth_context(app_key, callback_url)
        parsed_authorize = urlparse(auth_context.authorization_url)
        query = parse_qs(parsed_authorize.query, keep_blank_values=True)
        authorize_redirect_uri = query.get("redirect_uri", [""])[0]
        redacted_query = dict(query)
        redacted_query["client_id"] = [f"...{app_key[-4:]}"]
        if "state" in redacted_query:
            redacted_query["state"] = ["[REDACTED]"]
        authorize_url = parsed_authorize._replace(query=urlencode(redacted_query, doseq=True)).geturl()
        return {
            "callback_url": callback_url,
            "callback_scheme": parsed_callback.scheme,
            "callback_host": parsed_callback.hostname,
            "callback_port": parsed_callback.port,
            "callback_path": parsed_callback.path or "<empty>",
            "authorize_redirect_uri": authorize_redirect_uri,
            "authorize_url": authorize_url,
        }

    @staticmethod
    def _browser_opener(requested_browser: str | None = None) -> Callable[[str], bool]:
        if requested_browser:
            return webbrowser.get(requested_browser).open
        return webbrowser.open

    @staticmethod
    def _open_browser_target(
        target_url: str,
        open_browser: Callable[[str], bool],
        failure_message: str,
    ) -> None:
        try:
            opened = open_browser(target_url)
        except webbrowser.Error as exc:
            raise SchwabClientError(failure_message) from exc
        if not opened:
            raise SchwabClientError(failure_message)

    @classmethod
    def _callback_path_mismatch_message(cls, callback_url: str, request_path: str) -> str:
        expected_path = cls._normalize_callback_path(urlparse(callback_url).path)
        return (
            f"The browser reached the local callback listener, but requested path {request_path} instead of {expected_path}. "
            f"Confirm the Schwab developer app callback URL exactly matches {callback_url}."
        )

    def _run_callback_preflight(
        self,
        callback_url: str,
        timeout_seconds: float = CALLBACK_PREFLIGHT_TIMEOUT_SECONDS,
        open_browser: Callable[[str], bool] | None = None,
    ) -> None:
        callback_port, _, display_path = self._callback_components(callback_url)
        logger.info(
            "Starting local callback preflight callback_url=%s scheme=https host=127.0.0.1 port=%s path=%s",
            callback_url,
            callback_port,
            display_path,
        )
        opener = open_browser or webbrowser.open
        with self._callback_preflight_server(callback_url) as callback_queue:
            self._open_browser_target(
                callback_url,
                opener,
                "Unable to open the default browser for the local callback preflight.",
            )
            try:
                self._wait_for_received_url(callback_queue, timeout_seconds, callback_url=callback_url)
            except auth.RedirectTimeoutError as exc:
                raise SchwabClientError(
                    f"Local callback preflight failed for {callback_url}. "
                    "The browser never reached the local callback listener. "
                    "This points to local browser or certificate handling, not Schwab credentials. "
                    f"Open {callback_url} directly, accept the browser security warning, and confirm the callback page loads."
                ) from exc
        logger.info("Local callback preflight succeeded callback_url=%s", callback_url)

    def _prepare_login_attempt(self, creds: Credentials) -> tuple[Any, dict[str, Any]]:
        auth_context = self._build_auth_context(creds)
        diagnostics = self._authorization_diagnostics(creds.app_key, creds.callback_url, auth_context=auth_context)
        self._pending_auth_context = auth_context
        return auth_context, diagnostics

    def _consume_received_url(
        self,
        creds: Credentials,
        received_url: str,
        *,
        auth_context: Any | None = None,
    ) -> list[dict[str, Any]]:
        authorization_response = received_url.strip()
        if not authorization_response:
            raise SchwabClientError("Redirect URL is required to complete login.")
        self._validate_received_callback_url(creds.callback_url, authorization_response)

        active_auth_context = auth_context or self._pending_auth_context
        if active_auth_context is None:
            query = parse_qs(urlparse(authorization_response).query, keep_blank_values=True)
            if query.get("code") or query.get("state"):
                raise SchwabClientError(
                    "The browser reached the callback URL, but the saved authorization context was missing or reset before token exchange. "
                    "Start login again to generate a fresh authorization URL."
                )
            raise SchwabClientError("Login has not been started. Start login before completing the redirect URL.")

        try:
            self._connect_from_received_url(creds, active_auth_context, authorization_response)
            linked_accounts = self._verify_login_session()
        except SchwabClientError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise SchwabClientError(
                self._auth_error_message(
                    exc,
                    creds.callback_url,
                    received_url=authorization_response,
                )
            ) from exc
        self._pending_auth_context = None
        return linked_accounts

    def _connect_via_browser_callback(
        self,
        creds: Credentials,
        interactive: bool,
        callback_timeout: float,
        requested_browser: str | None = None,
    ):
        callback_port, callback_path, display_path = self._callback_components(creds.callback_url)
        auth_context, diagnostics = self._prepare_login_attempt(creds)
        opener = self._browser_opener(requested_browser)

        logger.info(
            "Starting local callback preflight callback_url=%s scheme=https host=127.0.0.1 port=%s path=%s",
            creds.callback_url,
            callback_port,
            display_path,
        )
        with self._callback_listener_server(creds.callback_url) as callback_queue:
            self._open_browser_target(
                creds.callback_url,
                opener,
                "Unable to open the default browser for the local callback preflight.",
            )
            try:
                preflight_request = self._wait_for_callback_request(callback_queue, CALLBACK_PREFLIGHT_TIMEOUT_SECONDS)
            except auth.RedirectTimeoutError as exc:
                raise SchwabClientError(
                    f"Local callback preflight failed for {creds.callback_url}. "
                    "The browser never reached the local callback listener. "
                    "This points to local browser or certificate handling, not Schwab credentials. "
                    f"Open {creds.callback_url} directly, accept the browser security warning, and confirm the callback page loads."
                ) from exc
            if not preflight_request.matched_callback:
                raise SchwabClientError(self._callback_path_mismatch_message(creds.callback_url, preflight_request.request_path))
            self._last_preflight_succeeded = True
            logger.info("Local callback preflight succeeded callback_url=%s", creds.callback_url)

            logger.info(
                "Starting browser-assisted Schwab login callback_url=%s scheme=%s host=%s port=%s path=%s interactive=%s",
                creds.callback_url,
                diagnostics["callback_scheme"],
                diagnostics["callback_host"],
                diagnostics["callback_port"],
                display_path,
                interactive,
            )
            logger.info("Schwab authorize preview authorize_url=%s", diagnostics["authorize_url"])
            logger.info(
                "Authorize redirect_uri matches callback=%s",
                diagnostics["authorize_redirect_uri"] == creds.callback_url,
            )
            self._open_browser_target(
                auth_context.authorization_url,
                opener,
                "Unable to open the default browser for the Schwab authorization URL.",
            )
            callback_request = self._wait_for_callback_request(callback_queue, callback_timeout)

        if callback_request.request_path != callback_path or not callback_request.matched_callback:
            raise SchwabClientError(self._callback_path_mismatch_message(creds.callback_url, callback_request.request_path))

        logger.info(
            "Received browser callback request callback_url=%s",
            redact_sensitive_data(callback_request.url, "received_url"),
        )
        self._consume_received_url(creds, callback_request.url, auth_context=auth_context)
        return self.client

    def _connect_from_received_url(self, creds: Credentials, auth_context: Any, received_url: str):
        self.client = auth.client_from_received_url(
            creds.app_key,
            creds.app_secret,
            auth_context,
            received_url,
            self._token_write,
            enforce_enums=False,
        )
        return self.client

    def _verify_login_session(self) -> list[dict[str, Any]]:
        linked_accounts = self.get_account_numbers()
        self._last_verified_accounts = list(linked_accounts)
        logger.info("Verified Schwab login linked_account_count=%s", len(linked_accounts))
        return linked_accounts

    def _auth_error_message(self, exc: Exception, callback_url: str, *, received_url: str | None = None) -> str:
        if isinstance(exc, auth.RedirectTimeoutError):
            if self._last_preflight_succeeded:
                return (
                    f"Timed out waiting for Schwab to redirect back to {callback_url}. "
                    "The local callback preflight succeeded, so the listener is reachable, but the browser never completed the final callback request during login. "
                    "The remaining likely causes are that the Schwab developer app callback URL does not exactly match this value, "
                    "or the browser did not complete the final redirect after Schwab approval."
                )
            return (
                f"Timed out waiting for Schwab to redirect back to {callback_url}. "
                "Confirm the Schwab developer app callback URL exactly matches this value and approve the local certificate warning if your browser shows one."
            )
        if isinstance(exc, auth.RedirectServerExitedError):
            port = urlparse(callback_url).port
            return (
                f"Unable to start the local callback server on {callback_url}. "
                f"Make sure nothing else is using port {port}, then try again."
            )
        message = str(exc)
        lowered = message.lower()
        if isinstance(exc, ValueError) and ("callback url" in lowered or "hostname" in lowered):
            return (
                f"Invalid callback URL for Python loopback auth: {callback_url}. "
                "It must use https, host 127.0.0.1, and an explicit port."
            )
        if any(token in lowered for token in ("redirect_uri", "redirect uri", "invalid_client", "invalid_grant", "unauthorized", "mismatch")):
            if received_url:
                return (
                    "Schwab reached the local callback URL, but rejected the OAuth code exchange. "
                    f"Confirm the Schwab developer app callback URL is exactly {callback_url}, and start a fresh login if this callback came from an earlier attempt."
                )
            return (
                "Schwab rejected the OAuth redirect or token exchange. "
                f"Confirm the Schwab developer app callback URL is exactly {callback_url}, including whether it omits a trailing slash."
            )
        if received_url:
            return f"Schwab reached the local callback URL, but the token exchange failed: {message}"
        return f"Schwab login failed: {message}"

    def _token_read(self) -> dict[str, Any]:
        payload = self.store.load()
        token = payload.get("token")
        if not token:
            raise FileNotFoundError("No stored Schwab token was found.")
        return token

    def _token_write(self, token: dict[str, Any], *args: Any, **kwargs: Any) -> None:
        payload = self.store.load()
        payload["token"] = token
        self.store.save(payload)

    def begin_manual_login(self) -> dict[str, Any]:
        creds = self._load_credentials()
        self._last_preflight_succeeded = None
        self._last_verified_accounts = []
        auth_context, diagnostics = self._prepare_login_attempt(creds)
        logger.info(
            "Prepared manual Schwab login callback_url=%s scheme=%s host=%s port=%s path=%s",
            creds.callback_url,
            diagnostics["callback_scheme"],
            diagnostics["callback_host"],
            diagnostics["callback_port"],
            diagnostics["callback_path"],
        )
        logger.info("Schwab authorize preview authorize_url=%s", diagnostics["authorize_url"])
        return {
            "authorization_url": auth_context.authorization_url,
            "diagnostics": diagnostics,
        }

    def complete_manual_login(self, received_url: str) -> list[dict[str, Any]]:
        creds = self._load_credentials()
        return self._consume_received_url(creds, received_url)

    def last_verified_accounts(self) -> list[dict[str, Any]]:
        return list(self._last_verified_accounts)

    def connect(
        self,
        force_login: bool = False,
        interactive: bool = True,
        callback_timeout: float = DEFAULT_CALLBACK_TIMEOUT_SECONDS,
        requested_browser: str | None = None,
    ):
        creds = self._load_credentials()
        payload = self.store.load()
        has_token = "token" in payload and not force_login
        self._last_preflight_succeeded = None
        self._last_verified_accounts = []
        self._pending_auth_context = None

        try:
            if has_token:
                self.client = self._connect_from_existing_token(creds)
                self._verify_login_session()
            else:
                self.client = self._connect_via_browser_callback(
                    creds,
                    interactive,
                    callback_timeout,
                    requested_browser=requested_browser,
                )
        except SchwabClientError:
            raise
        except Exception as exc:  # noqa: BLE001
            raise SchwabClientError(self._auth_error_message(exc, creds.callback_url)) from exc

        return self.client

    def ensure_client(self, interactive: bool = False):
        if self.client is None:
            self.connect(force_login=False, interactive=interactive)
        return self.client

    @staticmethod
    def _response_json(response: httpx.Response, message: str) -> Any:
        try:
            response.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            body = response.text
            raise SchwabClientError(f"{message}: HTTP {response.status_code} {response.reason_phrase}: {body}") from exc
        return response.json()

    def raw_request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        client = self.ensure_client()
        target = url if url.startswith("http") else f"{BASE_URL}{url}"
        response = client.session.request(method.upper(), target, **kwargs)
        return response

    def login_status(self) -> dict[str, Any]:
        payload = self.store.load()
        token = payload.get("token")
        token_age_seconds = None
        if isinstance(token, dict) and "creation_timestamp" in token:
            token_age_seconds = int(datetime.now().timestamp()) - int(token["creation_timestamp"])
        return {
            "configured": bool(payload.get("app_key") and payload.get("app_secret") and payload.get("callback_url")),
            "has_token": token is not None,
            "callback_url": payload.get("callback_url"),
            "token_age_seconds": token_age_seconds,
        }

    def get_account_numbers(self) -> list[dict[str, Any]]:
        response = self.ensure_client().get_account_numbers()
        payload = self._response_json(response, "Unable to load linked accounts")
        return list(payload)

    def get_account_hash_map(self) -> dict[str, str]:
        return {row["accountNumber"]: row["hashValue"] for row in self.get_account_numbers()}

    def get_user_preferences(self) -> dict[str, Any]:
        response = self.ensure_client().get_user_preferences()
        return self._response_json(response, "Unable to load user preferences")

    def get_account_nickname_map(self) -> dict[str, str]:
        prefs = self.get_user_preferences()
        accounts = prefs.get("accounts", [])
        return {entry.get("accountNumber", ""): entry.get("nickName", "") for entry in accounts}

    @staticmethod
    def _normalize_account_fields(fields: str | Iterable[str] | None) -> list[str] | None:
        if fields is None:
            return None
        if isinstance(fields, str):
            return [fields]
        return list(fields)

    def get_account(self, account_hash: str, fields: str | Iterable[str] | None = None) -> dict[str, Any]:
        normalized_fields = self._normalize_account_fields(fields)
        response = self.ensure_client().get_account(account_hash, fields=normalized_fields)
        return self._response_json(response, f"Unable to load account {account_hash}")

    def get_accounts_snapshot(self) -> list[AccountSnapshot]:
        nicknames = self.get_account_nickname_map()
        snapshots: list[AccountSnapshot] = []
        for account_number, account_hash in self.get_account_hash_map().items():
            details = self.get_account(account_hash)
            account = details.get("securitiesAccount", {})
            balances = account.get("currentBalances", {})
            cash_available_for_trading = balances.get("cashAvailableForTrading")
            cash_balance = balances.get("cashBalance")
            cash_available = cash_available_for_trading
            if cash_balance is not None and cash_available_for_trading is not None:
                cash_available = max(cash_balance, cash_available_for_trading)
            snapshots.append(
                AccountSnapshot(
                    account_name=nicknames.get(account_number) or account_number,
                    account_number=account_number,
                    account_hash=account_hash,
                    cash_available=cash_available,
                    liquidation_value=balances.get("liquidationValue"),
                    raw=details,
                )
            )
        return snapshots

    def get_positions_snapshot(self) -> list[PositionSnapshot]:
        nicknames = self.get_account_nickname_map()
        positions: list[PositionSnapshot] = []
        for account_number, account_hash in self.get_account_hash_map().items():
            details = self.get_account(account_hash, fields="positions")
            account = details.get("securitiesAccount", {})
            for position in account.get("positions", []) or []:
                instrument = position.get("instrument", {})
                if instrument.get("type") == "SWEEP_VEHICLE":
                    continue
                symbol = instrument.get("symbol")
                if not symbol:
                    continue
                long_quantity = position.get("longQuantity") or 0
                short_quantity = position.get("shortQuantity") or 0
                quantity = long_quantity + short_quantity
                average_price = position.get("averagePrice")
                value = average_price * quantity if average_price is not None else None
                positions.append(
                    PositionSnapshot(
                        account_name=nicknames.get(account_number) or account_number,
                        account_number=account_number,
                        account_hash=account_hash,
                        symbol=symbol,
                        average_price=average_price,
                        quantity=quantity,
                        value=value,
                        day_pl=position.get("currentDayProfitLoss"),
                        raw=position,
                    )
                )
        return positions

    def get_quotes(self, symbols: list[str]) -> dict[str, QuoteData]:
        unique_symbols = sorted({symbol.strip().upper() for symbol in symbols if symbol.strip()})
        if not unique_symbols:
            return {}
        response = self.ensure_client().get_quotes(unique_symbols)
        payload = self._response_json(response, "Unable to load quotes")
        quotes: dict[str, QuoteData] = {}
        for symbol, row in payload.items():
            quote = row.get("quote", {})
            quotes[symbol] = QuoteData(
                symbol=symbol,
                open_price=quote.get("openPrice"),
                high_price=quote.get("highPrice"),
                low_price=quote.get("lowPrice"),
                close_price=quote.get("closePrice"),
                last_price=quote.get("lastPrice") or quote.get("mark") or quote.get("askPrice"),
                mark_price=quote.get("mark"),
                bid_price=quote.get("bidPrice"),
                ask_price=quote.get("askPrice"),
                raw=row,
            )
        return quotes

    def preview_order(self, account_hash: str, order_spec: dict[str, Any]) -> httpx.Response:
        response = self.ensure_client().preview_order(account_hash, order_spec)
        return response

    def place_order(self, account_hash: str, order_spec: dict[str, Any]) -> httpx.Response:
        return self.ensure_client().place_order(account_hash, order_spec)

    def cancel_order(self, account_hash: str, order_id: str) -> httpx.Response:
        return self.ensure_client().cancel_order(order_id, account_hash)

    def get_order(self, account_hash: str, order_id: str) -> dict[str, Any]:
        response = self.ensure_client().get_order(order_id, account_hash)
        return self._response_json(response, f"Unable to load order {order_id}")

    def get_orders_snapshot(self, lookback_days: int) -> list[BrokerOrder]:
        nicknames = self.get_account_nickname_map()
        now = datetime.now().astimezone()
        start = now - timedelta(days=max(1, min(90, lookback_days)))
        snapshots: list[BrokerOrder] = []
        for account_number, account_hash in self.get_account_hash_map().items():
            response = self.ensure_client().get_orders_for_account(
                account_hash,
                from_entered_datetime=start,
                to_entered_datetime=now,
                max_results=1000,
            )
            orders = self._response_json(response, f"Unable to load orders for account {account_number}")
            if isinstance(orders, dict):
                iterable = orders.get("orders", [])
            else:
                iterable = orders

            for order in iterable:
                order_id = str(order.get("orderId", ""))
                if not order_id:
                    continue
                status_details = order.get("statusDescription") or ""
                cost_basis_method = order.get("taxLotMethod") or order.get("tax_lot_method")
                if not status_details or not cost_basis_method:
                    details = self.get_order(account_hash, order_id)
                    if not status_details:
                        status_details = details.get("statusDescription") or details.get("message") or ""
                        if not status_details:
                            status_details = json.dumps(details)[:500]
                    if not cost_basis_method:
                        cost_basis_method = details.get("taxLotMethod") or details.get("tax_lot_method")
                        if not cost_basis_method and isinstance(details.get("order"), dict):
                            cost_basis_method = details["order"].get("taxLotMethod") or details["order"].get("tax_lot_method")
                legs = order.get("orderLegCollection", [])
                symbol = None
                instruction = ""
                if legs:
                    symbol = legs[0].get("instrument", {}).get("symbol")
                    instruction = str(legs[0].get("instruction", "")).upper()
                quantity = order.get("quantity")
                if quantity is not None and instruction.startswith("SELL"):
                    quantity = -abs(quantity)
                elif quantity is not None and instruction.startswith("BUY"):
                    quantity = abs(quantity)
                snapshots.append(
                    BrokerOrder(
                        account_name=nicknames.get(account_number) or account_number,
                        account_number=account_number,
                        account_hash=account_hash,
                        order_id=order_id,
                        quantity=quantity,
                        symbol=symbol,
                        price=order.get("price"),
                        entered_time=order.get("enteredTime"),
                        time_in_force=order.get("duration"),
                        session=order.get("session"),
                        status=order.get("status"),
                        status_details=status_details,
                        cost_basis_method=str(normalize_tax_lot_method(cost_basis_method or "")),
                        cancelable=str(order.get("status", "")).upper() in {"WORKING", "QUEUED", "ACCEPTED"},
                        raw=order,
                    )
                )
        return snapshots
