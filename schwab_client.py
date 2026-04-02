from __future__ import annotations

import json
import logging
import multiprocessing
import os
import queue
import sys
import threading
import time
import webbrowser
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlencode, urlparse

import httpx
from schwab import auth

from order_builders import normalize_tax_lot_method
from schemas import AccountSnapshot, BrokerOrder, PositionSnapshot, QuoteData
from storage import CredentialStore, ensure_runtime_dirs, get_app_paths


BASE_URL = "https://api.schwabapi.com"
DEFAULT_CALLBACK_URL = "https://127.0.0.1:8182"
DEFAULT_CALLBACK_TIMEOUT_SECONDS = 300.0
LOGIN_TASK_TIMEOUT_SECONDS = int(DEFAULT_CALLBACK_TIMEOUT_SECONDS + 45)
CALLBACK_PREFLIGHT_TIMEOUT_SECONDS = 30.0
CALLBACK_PREFLIGHT_STATUS_PATH = "/schwab-tool-auth/status"
SCHWAB_PY_STATUS_PATH = "/schwab-py-internal/status"


logger = logging.getLogger("runner")


def _run_loopback_callback_preflight_server(
    callback_queue: multiprocessing.Queue[str],
    callback_port: int,
    callback_path: str,
) -> None:
    import flask

    app = flask.Flask(__name__)

    @app.route(callback_path)
    def handle_callback() -> str:
        callback_queue.put(flask.request.url)
        return "Schwab local callback probe received. You may now close this window/tab."

    @app.route(CALLBACK_PREFLIGHT_STATUS_PATH)
    def status() -> str:
        return "running"

    with open(os.devnull, "w", encoding="utf-8") as devnull:
        werkzeug_logger = logging.getLogger("werkzeug")
        werkzeug_logger.setLevel(logging.ERROR)
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            app.run(host="127.0.0.1", port=callback_port, ssl_context="adhoc", use_reloader=False)
        finally:
            sys.stdout = old_stdout


class SchwabClientError(RuntimeError):
    pass


@dataclass
class Credentials:
    app_key: str
    app_secret: str
    callback_url: str


class SchwabClient:
    def __init__(self, root: Path | None = None) -> None:
        self.paths = ensure_runtime_dirs(get_app_paths(root))
        self.store = CredentialStore(self.paths.credentials_path)
        self.client = None
        self._last_preflight_succeeded: bool | None = None

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
    def _callback_components(callback_url: str) -> tuple[int, str, str]:
        parsed = urlparse(callback_url)
        if parsed.port is None:
            raise SchwabClientError("Callback URL must include an explicit loopback port.")
        return parsed.port, parsed.path or "/", parsed.path or "<empty>"

    def _wait_for_https_server(self, *, status_url: str, timeout_seconds: float, server: multiprocessing.Process) -> None:
        deadline = time.time() + max(5.0, min(timeout_seconds, 15.0))
        while time.time() < deadline:
            if server.exitcode is not None:
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
    def _callback_preflight_server(self, callback_url: str) -> Any:
        callback_port, callback_path, _ = self._callback_components(callback_url)
        callback_queue: multiprocessing.Queue[str] = multiprocessing.Queue()
        server = multiprocessing.Process(
            target=_run_loopback_callback_preflight_server,
            args=(callback_queue, callback_port, callback_path),
        )
        server.start()
        try:
            self._wait_for_https_server(
                status_url=f"https://127.0.0.1:{callback_port}{CALLBACK_PREFLIGHT_STATUS_PATH}",
                timeout_seconds=CALLBACK_PREFLIGHT_TIMEOUT_SECONDS,
                server=server,
            )
            yield callback_queue
        finally:
            if server.is_alive():
                server.kill()
            server.join(timeout=5)

    def _wait_for_received_url(self, callback_queue: multiprocessing.Queue[str], timeout_seconds: float) -> str:
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            remaining = max(0.1, deadline - time.time())
            try:
                return callback_queue.get(timeout=min(remaining, 0.25))
            except queue.Empty:
                continue
        raise auth.RedirectTimeoutError(
            "Timed out waiting for a post-authorization callback. You can set a longer timeout by passing a value of callback_timeout."
        )

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
    def _authorization_diagnostics(app_key: str, callback_url: str) -> dict[str, Any]:
        parsed_callback = urlparse(callback_url)
        auth_context = auth.get_auth_context(app_key, callback_url)
        parsed_authorize = urlparse(auth_context.authorization_url)
        query = parse_qs(parsed_authorize.query, keep_blank_values=True)
        query["client_id"] = [f"...{app_key[-4:]}"]
        if "state" in query:
            query["state"] = ["[REDACTED]"]
        redacted_query = urlencode(query, doseq=True)
        return {
            "callback_url": callback_url,
            "callback_scheme": parsed_callback.scheme,
            "callback_host": parsed_callback.hostname,
            "callback_port": parsed_callback.port,
            "callback_path": parsed_callback.path or "<empty>",
            "authorize_redirect_uri": query.get("redirect_uri", [""])[0],
            "authorize_url": parsed_authorize._replace(query=redacted_query).geturl(),
        }

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
            opened = opener(callback_url)
            if not opened:
                raise SchwabClientError("Unable to open the default browser for the local callback preflight.")
            try:
                self._wait_for_received_url(callback_queue, timeout_seconds)
            except auth.RedirectTimeoutError as exc:
                raise SchwabClientError(
                    f"Local callback preflight failed for {callback_url}. "
                    "The browser never reached the local callback listener. "
                    "This points to local browser or certificate handling, not Schwab credentials. "
                    f"Open {callback_url} directly, accept the browser security warning, and confirm the callback page loads."
                ) from exc
        logger.info("Local callback preflight succeeded callback_url=%s", callback_url)

    def _monitor_schwab_py_status(self, callback_port: int, stop_event: threading.Event) -> None:
        status_url = f"https://127.0.0.1:{callback_port}{SCHWAB_PY_STATUS_PATH}"
        deadline = time.time() + 20.0
        while time.time() < deadline and not stop_event.is_set():
            try:
                response = httpx.get(status_url, verify=False, timeout=1.0)
            except httpx.HTTPError:
                time.sleep(0.1)
                continue
            if response.status_code == 200:
                logger.info("schwab-py callback listener reachable status_url=%s", status_url)
                return
            time.sleep(0.1)
        if not stop_event.is_set():
            logger.warning("schwab-py callback listener was not observed within the startup window status_url=%s", status_url)

    def _connect_via_login_flow(self, creds: Credentials, interactive: bool, callback_timeout: float):
        callback_port, _, display_path = self._callback_components(creds.callback_url)
        diagnostics = self._authorization_diagnostics(creds.app_key, creds.callback_url)
        logger.info(
            "Starting schwab-py login flow callback_url=%s scheme=%s host=%s port=%s path=%s",
            creds.callback_url,
            diagnostics["callback_scheme"],
            diagnostics["callback_host"],
            diagnostics["callback_port"],
            display_path,
        )
        logger.info("Schwab authorize preview authorize_url=%s", diagnostics["authorize_url"])
        logger.info(
            "Authorize redirect_uri matches callback=%s",
            diagnostics["authorize_redirect_uri"] == creds.callback_url,
        )

        stop_event = threading.Event()
        monitor = threading.Thread(
            target=self._monitor_schwab_py_status,
            args=(callback_port, stop_event),
            daemon=True,
        )
        monitor.start()
        try:
            self.client = auth.client_from_login_flow(
                creds.app_key,
                creds.app_secret,
                creds.callback_url,
                str(self.paths.state_dir / "schwab-py-unused-token.json"),
                enforce_enums=False,
                token_write_func=self._token_write,
                callback_timeout=callback_timeout,
                interactive=interactive,
            )
        finally:
            stop_event.set()
            monitor.join(timeout=1.0)
        return self.client

    def _auth_error_message(self, exc: Exception, callback_url: str) -> str:
        if isinstance(exc, auth.RedirectTimeoutError):
            if self._last_preflight_succeeded:
                return (
                    f"Timed out waiting for Schwab to redirect back to {callback_url}. "
                    "The local callback preflight succeeded, so the listener is reachable. "
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
            return (
                "Schwab rejected the OAuth redirect or token exchange. "
                f"Confirm the Schwab developer app callback URL is exactly {callback_url}, including whether it omits a trailing slash."
            )
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

    def connect(
        self,
        force_login: bool = False,
        interactive: bool = True,
        callback_timeout: float = DEFAULT_CALLBACK_TIMEOUT_SECONDS,
    ):
        creds = self._load_credentials()
        payload = self.store.load()
        has_token = "token" in payload and not force_login
        self._last_preflight_succeeded = None

        try:
            if has_token:
                self.client = self._connect_from_existing_token(creds)
            else:
                self._run_callback_preflight(creds.callback_url)
                self._last_preflight_succeeded = True
                self.client = self._connect_via_login_flow(creds, interactive, callback_timeout)
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

    def get_account(self, account_hash: str, fields: str | None = None) -> dict[str, Any]:
        response = self.ensure_client().get_account(account_hash, fields=fields)
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
