from __future__ import annotations

import argparse
import sys
from typing import Sequence

from logging_setup import setup_runner_logging
from schwab_client import DEFAULT_CALLBACK_TIMEOUT_SECONDS, SchwabClient, SchwabClientError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run an isolated Schwab login diagnostic.")
    parser.add_argument("mode", choices=("manual", "auto"), help="Which login path to exercise.")
    parser.add_argument("--app-key", dest="app_key", help="Schwab app key. Omit to use stored credentials.")
    parser.add_argument("--app-secret", dest="app_secret", help="Schwab app secret. Omit to use stored credentials.")
    parser.add_argument("--callback-url", dest="callback_url", help="Schwab callback URL. Omit to use stored credentials.")
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_CALLBACK_TIMEOUT_SECONDS,
        help=f"Callback timeout in seconds. Defaults to {DEFAULT_CALLBACK_TIMEOUT_SECONDS:g}.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Clear any stored token before running the diagnostic.",
    )
    parser.add_argument(
        "--browser",
        help="Optional browser override passed to the browser-assisted login flow.",
    )
    return parser


def _configure_client(args: argparse.Namespace, parser: argparse.ArgumentParser) -> SchwabClient:
    provided = [bool(args.app_key), bool(args.app_secret), bool(args.callback_url)]
    if any(provided) and not all(provided):
        parser.error("Provide --app-key, --app-secret, and --callback-url together, or omit all three to use stored credentials.")

    client = SchwabClient()
    if all(provided):
        client.save_credentials(args.app_key, args.app_secret, args.callback_url)
    if args.force_refresh:
        client.clear_token()
    return client


def _print_summary(mode: str, client: SchwabClient, linked_accounts: list[dict[str, object]]) -> None:
    login_status = client.login_status()
    print()
    print("Auth diagnostic complete")
    print(f"mode: {mode}")
    print(f"callback_url: {login_status.get('callback_url')}")
    print(f"token_present: {login_status.get('has_token')}")
    print(f"linked_accounts: {len(linked_accounts)}")


def _run_manual(client: SchwabClient) -> int:
    login = client.begin_manual_login()
    diagnostics = login["diagnostics"]
    print()
    print("Open this Schwab authorization URL in your browser:")
    print(login["authorization_url"])
    print()
    print("Redacted diagnostics:")
    for key in ("callback_url", "callback_port", "callback_path", "authorize_redirect_uri", "authorize_url"):
        print(f"{key}: {diagnostics[key]}")
    print()
    received_url = input("Redirect URL> ").strip()
    linked_accounts = client.complete_manual_login(received_url)
    _print_summary("manual", client, linked_accounts)
    return 0


def _run_auto(client: SchwabClient, timeout: float, browser: str | None) -> int:
    client.connect(
        force_login=True,
        interactive=False,
        callback_timeout=timeout,
        requested_browser=browser,
    )
    linked_accounts = client.last_verified_accounts() or client.get_account_numbers()
    _print_summary("auto", client, linked_accounts)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    setup_runner_logging()
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        client = _configure_client(args, parser)
        if args.mode == "manual":
            return _run_manual(client)
        return _run_auto(client, args.timeout, args.browser)
    except SchwabClientError as exc:
        print(f"Auth diagnostic failed: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Auth diagnostic canceled.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
