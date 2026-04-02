from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from schwab import auth

from order_builders import normalize_tax_lot_method
from schemas import AccountSnapshot, BrokerOrder, PositionSnapshot, QuoteData
from storage import CredentialStore, ensure_runtime_dirs, get_app_paths


BASE_URL = "https://api.schwabapi.com"
DEFAULT_CALLBACK_URL = "https://127.0.0.1:8182/"
DEFAULT_CALLBACK_TIMEOUT_SECONDS = 300.0
LOGIN_TASK_TIMEOUT_SECONDS = int(DEFAULT_CALLBACK_TIMEOUT_SECONDS + 45)


logger = logging.getLogger("runner")


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
            raise SchwabClientError("Callback URL must include an explicit loopback port, for example https://127.0.0.1:8182/.")
        if parsed.path and not parsed.path.startswith("/"):
            raise SchwabClientError("Callback URL path must start with '/'.")
        if parsed.query or parsed.fragment:
            raise SchwabClientError("Callback URL cannot contain query parameters or fragments.")

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
    def _auth_error_message(exc: Exception, callback_url: str) -> str:
        if isinstance(exc, auth.RedirectTimeoutError):
            return (
                f"Timed out waiting for Schwab to redirect back to {callback_url}. "
                "Confirm the Schwab developer app callback URL exactly matches this value and approve the local certificate warning if your browser shows one."
            )
        if isinstance(exc, auth.RedirectServerExitedError):
            return (
                f"Unable to start the local callback server on {callback_url}. "
                "Make sure nothing else is using port 8182, then try again."
            )
        message = str(exc)
        lowered = message.lower()
        if isinstance(exc, ValueError) and ("callback url" in lowered or "hostname" in lowered):
            return f"Invalid callback URL. It must exactly match {DEFAULT_CALLBACK_URL}."
        if any(token in lowered for token in ("redirect_uri", "redirect uri", "invalid_client", "invalid_grant", "unauthorized", "mismatch")):
            return (
                "Schwab rejected the OAuth redirect or token exchange. "
                f"Confirm the Schwab developer app callback URL is exactly {DEFAULT_CALLBACK_URL}, including the trailing slash."
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

        try:
            if has_token:
                self.client = self._connect_from_existing_token(creds)
            else:
                self.client = auth.client_from_login_flow(
                    creds.app_key,
                    creds.app_secret,
                    creds.callback_url,
                    str(self.paths.state_dir / "schwab-token.json"),
                    enforce_enums=False,
                    token_write_func=self._token_write,
                    callback_timeout=callback_timeout,
                    interactive=interactive,
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
