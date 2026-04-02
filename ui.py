from __future__ import annotations

import json
import queue
import subprocess
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

from imports import create_import_template
from logging_setup import setup_ui_logging
from schwab_client import DEFAULT_CALLBACK_URL, LOGIN_TASK_TIMEOUT_SECONDS, SchwabClient, SchwabClientError
from schemas import (
    Duration,
    ExecutionProfile,
    LimitPricingMethod,
    OrderTemplate,
    Session,
    SortPreset,
    TaxLotMethod,
    TaskRequest,
)
from storage import CredentialStore, ensure_runtime_dirs, get_app_paths, load_settings, sanitize_runtime_artifacts, save_setting


logger = setup_ui_logging()


class DataTable(ttk.Frame):
    def __init__(self, master: tk.Misc) -> None:
        super().__init__(master)
        self.tree = ttk.Treeview(self, show="headings")
        yscroll = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        xscroll = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

    def set_rows(self, columns: list[str], rows: list[dict[str, Any]]) -> None:
        self.tree.delete(*self.tree.get_children())
        self.tree["columns"] = columns
        for column in columns:
            self.tree.heading(column, text=column.replace("_", " ").upper())
            self.tree.column(column, width=max(110, len(column) * 10), stretch=True)
        for row in rows:
            values = [self._format_value(row.get(column)) for column in columns]
            self.tree.insert("", "end", values=values)

    @staticmethod
    def _format_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float):
            return f"{value:.6f}".rstrip("0").rstrip(".")
        if isinstance(value, list):
            return " | ".join(str(item) for item in value)
        if isinstance(value, dict):
            return json.dumps(value)
        return str(value)


class SchwabToolApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.paths = ensure_runtime_dirs(get_app_paths())
        sanitize_runtime_artifacts(self.paths)
        self.store = CredentialStore(self.paths.credentials_path)
        self.python_exe = self.paths.root / ".venv" / "Scripts" / "python.exe"
        self.runner_path = self.paths.root / "runner.py"
        self.queue: queue.Queue[tuple[str, dict[str, Any]]] = queue.Queue()
        self.busy = False
        self.current_preview_rows: list[dict[str, Any]] = []
        self.current_job_id = ""

        self.root.title("Schwab Tool")
        self.root.geometry("1560x920")
        self.root.minsize(1280, 760)

        self._load_initial_state()
        self._build_layout()
        self._refresh_logs_view()
        self.root.after(150, self._drain_queue)

    def _load_initial_state(self) -> None:
        settings = load_settings()
        credentials = self.store.load()
        saved_profile = settings.get("execution_profile", {})
        profile = ExecutionProfile.model_validate(saved_profile) if saved_profile else ExecutionProfile()

        self.import_path_var = tk.StringVar(value=settings.get("import_path", ""))
        self.app_key_var = tk.StringVar(value=credentials.get("app_key", ""))
        self.app_secret_var = tk.StringVar(value=credentials.get("app_secret", ""))
        self.callback_url_var = tk.StringVar(value=credentials.get("callback_url", DEFAULT_CALLBACK_URL))
        self.login_status_var = tk.StringVar(value="Token present" if credentials.get("token") else "Not logged in")

        self.order_template_var = tk.StringVar(value=profile.order_template.value)
        self.duration_var = tk.StringVar(value=profile.duration.value)
        self.session_var = tk.StringVar(value=profile.session.value)
        self.tax_lot_var = tk.StringVar(value=profile.tax_lot_method.value)
        self.sort_preset_var = tk.StringVar(value=profile.sort_preset.value)
        self.preview_only_var = tk.BooleanVar(value=profile.preview_only)
        self.pricing_method_var = tk.StringVar(value=profile.limit_pricing_method.value)
        self.legacy_d_min_var = tk.StringVar(value=str(profile.pricing_params.legacy_d_min))
        self.legacy_k_var = tk.StringVar(value=str(profile.pricing_params.legacy_k))
        self.delta_cap_bps_var = tk.StringVar(value=str(profile.pricing_params.delta_cap_bps))
        self.tick_cap_var = tk.StringVar(value="" if profile.pricing_params.tick_cap is None else str(profile.pricing_params.tick_cap))
        self.lookback_days_var = tk.StringVar(value=str(profile.orders_lookback_days))

        self.current_job_var = tk.StringVar(value="Idle")
        self.last_action_var = tk.StringVar(value="Ready")
        self.counts_var = tk.StringVar(value="Loaded 0 | Valid 0 | Submitted 0 | Rejected 0 | Canceled 0")
        self.refresh_var = tk.StringVar(value="No refresh yet")

    def _build_layout(self) -> None:
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)

        self._build_action_bar()

        body = ttk.Frame(self.root, padding=(12, 0, 12, 0))
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        left = ttk.Frame(body, padding=(0, 0, 12, 0))
        left.grid(row=0, column=0, sticky="ns")
        right = ttk.Frame(body)
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        self._build_left_rail(left)
        self._build_tabs(right)
        self._build_status_strip()

    def _build_action_bar(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.grid(row=0, column=0, sticky="ew")
        for column in range(11):
            frame.columnconfigure(column, weight=0)
        frame.columnconfigure(0, weight=1)

        ttk.Entry(frame, textvariable=self.import_path_var).grid(row=0, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(frame, text="Import File", command=self._choose_import_file).grid(row=0, column=1, padx=4)
        ttk.Button(frame, text="Validate", command=self._validate_import).grid(row=0, column=2, padx=4)
        ttk.Button(frame, text="Login / Refresh Auth", command=self._login).grid(row=0, column=3, padx=4)
        ttk.Button(frame, text="Refresh Accounts", command=self._refresh_accounts).grid(row=0, column=4, padx=4)
        ttk.Button(frame, text="Refresh Quotes", command=self._refresh_quotes).grid(row=0, column=5, padx=4)
        ttk.Button(frame, text="Place Orders", command=self._place_orders).grid(row=0, column=6, padx=4)
        ttk.Button(frame, text="Refresh Orders", command=self._refresh_orders).grid(row=0, column=7, padx=4)
        ttk.Button(frame, text="Refresh Portfolio", command=self._refresh_portfolio).grid(row=0, column=8, padx=4)
        ttk.Button(frame, text="Export Snapshot", command=self._export_snapshot).grid(row=0, column=9, padx=4)
        ttk.Button(frame, text="Open Template", command=self._open_template).grid(row=0, column=10, padx=(12, 0))

    def _build_left_rail(self, parent: ttk.Frame) -> None:
        parent.columnconfigure(0, weight=1)

        auth_frame = ttk.LabelFrame(parent, text="Auth", padding=10)
        auth_frame.grid(row=0, column=0, sticky="ew", pady=(0, 12))
        auth_frame.columnconfigure(1, weight=1)
        ttk.Label(auth_frame, text="App Key").grid(row=0, column=0, sticky="w")
        ttk.Entry(auth_frame, textvariable=self.app_key_var, width=34).grid(row=0, column=1, sticky="ew")
        ttk.Label(auth_frame, text="App Secret").grid(row=1, column=0, sticky="w")
        ttk.Entry(auth_frame, textvariable=self.app_secret_var, show="*", width=34).grid(row=1, column=1, sticky="ew")
        ttk.Label(auth_frame, text="Callback URI").grid(row=2, column=0, sticky="w")
        ttk.Entry(auth_frame, textvariable=self.callback_url_var, width=34).grid(row=2, column=1, sticky="ew")
        ttk.Label(auth_frame, text="Login Status").grid(row=3, column=0, sticky="w")
        ttk.Label(auth_frame, textvariable=self.login_status_var, width=30).grid(row=3, column=1, sticky="w")

        execution = ttk.LabelFrame(parent, text="Execution", padding=10)
        execution.grid(row=1, column=0, sticky="ew", pady=(0, 12))
        execution.columnconfigure(1, weight=1)
        ttk.Label(execution, text="Order Template").grid(row=0, column=0, sticky="w")
        ttk.Combobox(execution, textvariable=self.order_template_var, values=[item.value for item in OrderTemplate], state="readonly").grid(row=0, column=1, sticky="ew")
        ttk.Label(execution, text="Duration").grid(row=1, column=0, sticky="w")
        ttk.Combobox(execution, textvariable=self.duration_var, values=[item.value for item in Duration], state="readonly").grid(row=1, column=1, sticky="ew")
        ttk.Label(execution, text="Session").grid(row=2, column=0, sticky="w")
        ttk.Combobox(execution, textvariable=self.session_var, values=[item.value for item in Session], state="readonly").grid(row=2, column=1, sticky="ew")
        ttk.Label(execution, text="Tax Lot").grid(row=3, column=0, sticky="w")
        ttk.Combobox(execution, textvariable=self.tax_lot_var, values=[item.value for item in TaxLotMethod], state="readonly").grid(row=3, column=1, sticky="ew")
        ttk.Label(execution, text="Sort Preset").grid(row=4, column=0, sticky="w")
        ttk.Combobox(execution, textvariable=self.sort_preset_var, values=[item.value for item in SortPreset], state="readonly").grid(row=4, column=1, sticky="ew")
        ttk.Checkbutton(execution, text="Preview only", variable=self.preview_only_var).grid(row=5, column=0, columnspan=2, sticky="w")

        pricing = ttk.LabelFrame(parent, text="Limit Pricing", padding=10)
        pricing.grid(row=2, column=0, sticky="ew", pady=(0, 12))
        pricing.columnconfigure(1, weight=1)
        ttk.Label(pricing, text="Method").grid(row=0, column=0, sticky="w")
        ttk.Combobox(pricing, textvariable=self.pricing_method_var, values=[item.value for item in LimitPricingMethod], state="readonly").grid(row=0, column=1, sticky="ew")
        ttk.Label(pricing, text="Legacy d_min").grid(row=1, column=0, sticky="w")
        ttk.Entry(pricing, textvariable=self.legacy_d_min_var).grid(row=1, column=1, sticky="ew")
        ttk.Label(pricing, text="Legacy k").grid(row=2, column=0, sticky="w")
        ttk.Entry(pricing, textvariable=self.legacy_k_var).grid(row=2, column=1, sticky="ew")
        ttk.Label(pricing, text="Cap (bps)").grid(row=3, column=0, sticky="w")
        ttk.Entry(pricing, textvariable=self.delta_cap_bps_var).grid(row=3, column=1, sticky="ew")
        ttk.Label(pricing, text="Tick Cap").grid(row=4, column=0, sticky="w")
        ttk.Entry(pricing, textvariable=self.tick_cap_var).grid(row=4, column=1, sticky="ew")

        refresh = ttk.LabelFrame(parent, text="Status Refresh", padding=10)
        refresh.grid(row=3, column=0, sticky="ew")
        refresh.columnconfigure(1, weight=1)
        ttk.Label(refresh, text="Lookback Days").grid(row=0, column=0, sticky="w")
        ttk.Entry(refresh, textvariable=self.lookback_days_var).grid(row=0, column=1, sticky="ew")

    def _build_tabs(self, parent: ttk.Frame) -> None:
        self.notebook = ttk.Notebook(parent)
        self.notebook.grid(row=0, column=0, sticky="nsew")

        self.orders_table = DataTable(self.notebook)
        self.accounts_table = DataTable(self.notebook)
        self.order_status_table = DataTable(self.notebook)
        self.portfolio_table = DataTable(self.notebook)

        logs_frame = ttk.Frame(self.notebook)
        logs_frame.columnconfigure(0, weight=1)
        logs_frame.rowconfigure(0, weight=1)
        self.logs_text = tk.Text(logs_frame, wrap="none")
        self.log_yscroll = ttk.Scrollbar(logs_frame, orient="vertical", command=self.logs_text.yview)
        self.logs_text.grid(row=0, column=0, sticky="nsew")
        self.log_yscroll.grid(row=0, column=1, sticky="ns")
        self.logs_text.configure(yscrollcommand=self.log_yscroll.set)

        self.notebook.add(self.orders_table, text="Orders Preview")
        self.notebook.add(self.accounts_table, text="Accounts")
        self.notebook.add(self.order_status_table, text="Order Status")
        self.notebook.add(self.portfolio_table, text="Portfolio")
        self.notebook.add(logs_frame, text="Logs")

    def _build_status_strip(self) -> None:
        frame = ttk.Frame(self.root, padding=12)
        frame.grid(row=2, column=0, sticky="ew")
        frame.columnconfigure(4, weight=1)
        ttk.Label(frame, textvariable=self.current_job_var).grid(row=0, column=0, sticky="w", padx=(0, 16))
        ttk.Label(frame, textvariable=self.last_action_var).grid(row=0, column=1, sticky="w", padx=(0, 16))
        ttk.Label(frame, textvariable=self.counts_var).grid(row=0, column=2, sticky="w", padx=(0, 16))
        ttk.Label(frame, textvariable=self.refresh_var).grid(row=0, column=3, sticky="w", padx=(0, 16))
        self.progress = ttk.Progressbar(frame, mode="indeterminate")
        self.progress.grid(row=0, column=4, sticky="ew")

    def _execution_profile_payload(self) -> dict[str, Any]:
        tick_cap = self.tick_cap_var.get().strip()
        return {
            "order_template": self.order_template_var.get(),
            "duration": self.duration_var.get(),
            "session": self.session_var.get(),
            "tax_lot_method": self.tax_lot_var.get(),
            "limit_pricing_method": self.pricing_method_var.get(),
            "pricing_params": {
                "legacy_d_min": float(self.legacy_d_min_var.get() or "0.0005"),
                "legacy_k": float(self.legacy_k_var.get() or "3"),
                "delta_cap_bps": float(self.delta_cap_bps_var.get() or "25"),
                "tick_cap": int(tick_cap) if tick_cap else None,
            },
            "sort_preset": self.sort_preset_var.get(),
            "preview_only": bool(self.preview_only_var.get()),
            "orders_lookback_days": int(self.lookback_days_var.get() or "7"),
        }

    def _queue_task(self, task_name: str, args: dict[str, Any], *, timeout_seconds: int = 900) -> bool:
        if self.busy:
            messagebox.showwarning("Busy", "A task is already running.")
            return False

        request = TaskRequest(task_name=task_name, args=args, meta={"ui_version": "1.0"})
        request_path = self.paths.jobs_dir / f"{request.request_id}.json"
        result_path = self.paths.results_dir / f"{request.request_id}.json"
        request_path.write_text(json.dumps(request.model_dump(mode="json"), indent=2), encoding="utf-8")
        save_setting("import_path", self.import_path_var.get().strip())

        self.busy = True
        self.current_job_id = request.request_id
        self.current_job_var.set(f"Job {request.request_id}")
        callback_url = self.callback_url_var.get().strip() or DEFAULT_CALLBACK_URL
        if task_name == "login" and bool(args.get("force_login")):
            self.last_action_var.set("Opening browser for Schwab login")
            self.refresh_var.set(f"Waiting for redirect on {callback_url}")
            self.login_status_var.set(f"Approve the local certificate warning on {callback_url} if your browser shows it")
        elif task_name == "login":
            self.last_action_var.set("Refreshing saved Schwab session")
            self.refresh_var.set("Checking saved token and linked accounts")
            self.login_status_var.set("Using saved encrypted credentials")
        else:
            self.last_action_var.set(f"Running {task_name}")
        self.progress.start(12)

        logger.info("Starting task %s request_id=%s", task_name, request.request_id)

        def worker() -> None:
            command = [str(self.python_exe), str(self.runner_path), "--task", task_name, "--request-file", str(request_path), "--result-file", str(result_path)]
            try:
                process = subprocess.run(
                    command,
                    cwd=self.paths.root,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                )
                result = json.loads(result_path.read_text(encoding="utf-8")) if result_path.exists() else {
                    "status": "error",
                    "error": {"message": process.stderr or "Runner did not write a result file."},
                }
                self.queue.put(("result", {"task_name": task_name, "request_id": request.request_id, "payload": result, "stdout": process.stdout, "stderr": process.stderr}))
            except subprocess.TimeoutExpired:
                timeout_message = "Runner timed out."
                if task_name == "login":
                    timeout_message = (
                        f"Timed out waiting for Schwab to redirect back to {callback_url}. "
                        "Confirm the Schwab developer app callback URL exactly matches this value, including whether it omits a trailing slash, "
                        "and approve the local certificate warning if your browser shows it."
                    )
                self.queue.put(("result", {"task_name": task_name, "request_id": request.request_id, "payload": {"status": "timeout", "error": {"message": timeout_message}}}))
            except Exception as exc:  # noqa: BLE001
                self.queue.put(("result", {"task_name": task_name, "request_id": request.request_id, "payload": {"status": "error", "error": {"message": str(exc)}}}))

        threading.Thread(target=worker, daemon=True).start()
        return True

    def _drain_queue(self) -> None:
        try:
            while True:
                kind, payload = self.queue.get_nowait()
                if kind == "result":
                    self._handle_task_result(payload)
        except queue.Empty:
            pass
        self.root.after(150, self._drain_queue)

    def _handle_task_result(self, payload: dict[str, Any]) -> None:
        self.busy = False
        self.progress.stop()
        result = payload.get("payload", {})
        status = result.get("status")
        output = result.get("output") or {}
        error = result.get("error") or {}
        task_name = payload["task_name"]

        logger.info("Task %s finished with status %s", task_name, status)
        self._refresh_logs_view()

        if status != "success":
            self.last_action_var.set(f"{task_name} failed")
            if task_name == "login":
                self.login_status_var.set("Login failed")
            messagebox.showerror("Task failed", error.get("message", "Unknown error"))
            return

        self.last_action_var.set(f"{task_name} complete")
        self.refresh_var.set(f"Last refresh: {task_name}")

        if task_name == "login":
            login_status = output.get("login_status", {})
            self.login_status_var.set(
                f"Token {'present' if login_status.get('has_token') else 'missing'} | Accounts {output.get('linked_account_count', 0)}"
            )
        elif task_name in {"validate_import", "refresh_quotes", "place_orders"}:
            self.current_preview_rows = output.get("preview_rows", [])
            self._render_orders_preview(self.current_preview_rows)
        elif task_name == "refresh_accounts":
            self.accounts_table.set_rows(
                ["account_name", "account_number", "cash_available", "liquidation_value"],
                output.get("accounts", []),
            )
        elif task_name == "refresh_orders":
            orders = output.get("orders", [])
            self.order_status_table.set_rows(
                [
                    "account_name",
                    "account_number",
                    "order_id",
                    "status",
                    "quantity",
                    "symbol",
                    "price",
                    "entered_time",
                    "time_in_force",
                    "session",
                    "cost_basis_method",
                    "status_details",
                    "cancelable",
                ],
                orders,
            )
            self._merge_broker_statuses(orders)
        elif task_name == "refresh_portfolio":
            self.portfolio_table.set_rows(
                ["account_name", "account_number", "symbol", "average_price", "quantity", "value", "day_pl"],
                output.get("positions", []),
            )
        elif task_name == "export_snapshot":
            messagebox.showinfo("Export complete", output.get("export_path", "Export written."))

    def _render_orders_preview(self, rows: list[dict[str, Any]]) -> None:
        columns = [
            "row_number",
            "account_number",
            "symbol",
            "quantity",
            "bid_price",
            "ask_price",
            "last_price",
            "chosen_limit_price",
            "estimated_notional",
            "execution_sequence",
            "local_status",
            "order_id",
            "broker_status",
            "broker_status_detail",
            "validation_errors",
        ]
        self.orders_table.set_rows(columns, rows)
        valid_count = sum(1 for row in rows if not row.get("validation_errors") and row.get("enabled", True))
        invalid_count = sum(1 for row in rows if row.get("validation_errors"))
        submitted = sum(1 for row in rows if row.get("local_status") == "SUBMITTED")
        rejected = sum(1 for row in rows if row.get("local_status") == "REJECTED")
        self.counts_var.set(
            f"Loaded {len(rows)} | Valid {valid_count} | Invalid {invalid_count} | Submitted {submitted} | Rejected {rejected} | Canceled 0"
        )

    def _merge_broker_statuses(self, orders: list[dict[str, Any]]) -> None:
        if not self.current_preview_rows:
            return
        orders_by_id = {row.get("order_id"): row for row in orders if row.get("order_id")}
        for row in self.current_preview_rows:
            order_id = row.get("order_id")
            if order_id and order_id in orders_by_id:
                broker = orders_by_id[order_id]
                row["broker_status"] = broker.get("status")
                row["broker_status_detail"] = broker.get("status_details")
                row["broker_quantity"] = broker.get("quantity")
        self._render_orders_preview(self.current_preview_rows)

    def _refresh_logs_view(self) -> None:
        ui_log = self.paths.logs_dir / "ui.log"
        runner_log = self.paths.logs_dir / "runner.log"
        audit_log = self.paths.logs_dir / "audit.jsonl"
        chunks: list[str] = []
        for label, path in (("UI", ui_log), ("RUNNER", runner_log), ("AUDIT", audit_log)):
            if path.exists():
                lines = path.read_text(encoding="utf-8").splitlines()[-120:]
                chunks.append(f"===== {label} =====")
                chunks.extend(lines)
        self.logs_text.delete("1.0", "end")
        self.logs_text.insert("1.0", "\n".join(chunks))

    def _choose_import_file(self) -> None:
        selected = filedialog.askopenfilename(
            title="Choose import workbook",
            filetypes=[("Excel workbooks", "*.xlsx *.xlsm"), ("All files", "*.*")],
            initialdir=self.paths.imports_dir,
        )
        if selected:
            self.import_path_var.set(selected)

    def _open_template(self) -> None:
        template_path = self.paths.imports_dir / "order_template.xlsx"
        if not template_path.exists():
            create_import_template(template_path)
        self.import_path_var.set(str(template_path))
        messagebox.showinfo("Template ready", str(template_path))

    def _validate_import(self) -> None:
        if not self.import_path_var.get().strip():
            messagebox.showwarning("Import file", "Choose an import workbook first.")
            return
        self._queue_task(
            "validate_import",
            {
                "import_path": self.import_path_var.get().strip(),
                "execution_profile": self._execution_profile_payload(),
            },
        )

    def _login(self) -> None:
        if self.busy:
            messagebox.showwarning("Busy", "A task is already running.")
            return

        app_key = self.app_key_var.get().strip()
        app_secret = self.app_secret_var.get().strip()
        callback_url = self.callback_url_var.get().strip()

        if not app_key or not app_secret:
            messagebox.showwarning("Auth", "App key and app secret are required before login.")
            return
        try:
            SchwabClient._validate_callback_url(callback_url)
        except SchwabClientError as exc:
            messagebox.showerror("Callback URL mismatch", str(exc))
            return

        current_credentials = self.store.load()
        credentials_changed = any(
            current_credentials.get(field, "") != value
            for field, value in (
                ("app_key", app_key),
                ("app_secret", app_secret),
                ("callback_url", callback_url),
            )
        )
        force_login = credentials_changed or "token" not in current_credentials
        self._queue_task(
            "login",
            {
                "app_key": app_key,
                "app_secret": app_secret,
                "callback_url": callback_url,
                "force_login": force_login,
            },
            timeout_seconds=LOGIN_TASK_TIMEOUT_SECONDS,
        )

    def _refresh_accounts(self) -> None:
        self._queue_task("refresh_accounts", {})

    def _refresh_quotes(self) -> None:
        if not self.import_path_var.get().strip():
            messagebox.showwarning("Import file", "Choose an import workbook first.")
            return
        self._queue_task(
            "refresh_quotes",
            {
                "import_path": self.import_path_var.get().strip(),
                "execution_profile": self._execution_profile_payload(),
            },
        )

    def _place_orders(self) -> None:
        if not self.import_path_var.get().strip():
            messagebox.showwarning("Import file", "Choose an import workbook first.")
            return
        self._queue_task(
            "place_orders",
            {
                "import_path": self.import_path_var.get().strip(),
                "execution_profile": self._execution_profile_payload(),
            },
        )

    def _refresh_orders(self) -> None:
        self._queue_task("refresh_orders", {"execution_profile": self._execution_profile_payload()})

    def _refresh_portfolio(self) -> None:
        self._queue_task("refresh_portfolio", {})

    def _export_snapshot(self) -> None:
        self._queue_task("export_snapshot", {"execution_profile": self._execution_profile_payload()})


def main() -> None:
    root = tk.Tk()
    template_path = ensure_runtime_dirs(get_app_paths()).imports_dir / "order_template.xlsx"
    if not template_path.exists():
        create_import_template(template_path)
    app = SchwabToolApp(root)
    logger.info("UI started")
    root.mainloop()
    _ = app


if __name__ == "__main__":
    main()
