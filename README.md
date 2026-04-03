# Schwab Tool

Local Python port of the Excel/VBA Schwab workflow.

## Run

```powershell
& ".\.venv\Scripts\python.exe" ".\ui.py"
```

## Auth Diagnostics

Run the isolated auth helper when you want to debug login without exercising the rest of the app:

```powershell
& ".\.venv\Scripts\python.exe" ".\auth_diagnostic.py" manual --force-refresh
& ".\.venv\Scripts\python.exe" ".\auth_diagnostic.py" auto --force-refresh
```

- `manual` prints the generated Schwab authorization URL, then asks you to paste the final redirect URL after login.
- `auto` exercises the normal loopback callback capture path.
- `--browser <name>` optionally passes a browser override through to the loopback login flow; the default remains your installed browser.

The production login path should continue to use the installed browser by default. A real browser is a better fit than Playwright here because Schwab's OAuth flow depends on normal user-driven login, 2FA, trust prompts, and accepting the loopback certificate warning.

## First-use checklist

1. Update the Schwab app callback URI to an explicit loopback URL such as `https://127.0.0.1:8182`.
2. Open the app.
3. Enter `App Key`, `App Secret`, and `Callback URI`.
4. Click `Login / Refresh Auth`.
5. Use `imports\order_template.xlsx` as the starting import template.
6. If your browser shows a certificate warning for the local callback URL, approve it so the loopback redirect can complete.

The callback URL must exactly match the Schwab developer app entry, including whether it omits a trailing slash. The current app expects the no-trailing-slash form, for example `https://127.0.0.1:8182`.

## Layout

- `ui.py`: Tkinter desktop shell
- `runner.py`: subprocess task executor
- `tasks.py`: application tasks
- `schwab_client.py`: `schwab-py` wrapper + encrypted token handling
- `order_builders.py`: order-spec construction
- `pricing.py`: legacy and baseline NBBO pricing
- `imports.py`: import parsing + template generation
- `storage.py`: DPAPI secret storage + SQLite state
- `logs\`: UI, runner, and audit logs
- `state\app.db`: local snapshots and placed-order state
- `state\credentials.json.dpapi`: encrypted credentials and token data

## Notes

- `Validate` is structural validation only; it may mark rows as `NEEDS_QUOTES` when quote-derived limit pricing is required.
- `Refresh Quotes` computes preview pricing and execution order.
- `Place Orders` submits in the computed execution sequence.
- `Refresh Orders` is the broker-source-of-truth status view and backfills rejection details plus tax-lot data when needed.

## Tests

```powershell
& "C:\ProgramData\.pyenv\pyenv-win\versions\3.13.3\python.exe" -m pytest .\tests
```
