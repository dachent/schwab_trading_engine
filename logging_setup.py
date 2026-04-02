from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler

from storage import ensure_runtime_dirs, get_app_paths


def _build_logger(name: str, filename: str) -> logging.Logger:
    paths = ensure_runtime_dirs(get_app_paths())
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = RotatingFileHandler(
        paths.logs_dir / filename,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger


def setup_ui_logging() -> logging.Logger:
    return _build_logger("ui", "ui.log")


def setup_runner_logging() -> logging.Logger:
    return _build_logger("runner", "runner.log")

