from __future__ import annotations

import argparse
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

from logging_setup import setup_runner_logging
from schemas import ErrorInfo, TaskMetrics, TaskRequest, TaskResult, TaskStatus
from storage import append_audit_record, atomic_write_json, init_db, read_json, redact_sensitive_data
from tasks import TASKS


logger = setup_runner_logging()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", required=True)
    parser.add_argument("--request-file", required=True)
    parser.add_argument("--result-file", required=True)
    args = parser.parse_args()

    request_path = Path(args.request_file)
    result_path = Path(args.result_file)
    init_db()

    try:
        request = TaskRequest.model_validate(read_json(request_path))
    except Exception as exc:  # noqa: BLE001
        logger.exception("Invalid task request")
        started_at = finished_at = datetime.now().astimezone().isoformat()
        dummy = TaskRequest(task_name=args.task)
        result = TaskResult(
            request_id=dummy.request_id,
            task_name=args.task,
            started_at=started_at,
            finished_at=finished_at,
            status=TaskStatus.INVALID_INPUT,
            return_code=2,
            output=None,
            error=ErrorInfo(type=type(exc).__name__, message=str(exc), traceback=traceback.format_exc()),
            metrics=TaskMetrics(duration_ms=0),
        )
        atomic_write_json(result_path, redact_sensitive_data(result.model_dump(mode="json")))
        append_audit_record({"request": {"task_name": args.task}, "result": result.model_dump(mode="json")})
        return 2

    started_at = datetime.now().astimezone().isoformat()
    logger.info("Running task %s request_id=%s", request.task_name, request.request_id)

    if request.task_name not in TASKS:
        result = TaskResult(
            request_id=request.request_id,
            task_name=request.task_name,
            started_at=started_at,
            finished_at=datetime.now().astimezone().isoformat(),
            status=TaskStatus.INVALID_INPUT,
            return_code=3,
            output=None,
            error=ErrorInfo(type="TaskNotFound", message=f"Unknown task: {request.task_name}"),
            metrics=TaskMetrics(duration_ms=max(0, int((time.time() - _START_TIME) * 1000))),
        )
        atomic_write_json(result_path, redact_sensitive_data(result.model_dump(mode="json")))
        append_audit_record({"request": request.model_dump(mode="json"), "result": result.model_dump(mode="json")})
        return 3

    try:
        output = TASKS[request.task_name](request.args)
        result = TaskResult(
            request_id=request.request_id,
            task_name=request.task_name,
            started_at=started_at,
            finished_at=datetime.now().astimezone().isoformat(),
            status=TaskStatus.SUCCESS,
            return_code=0,
            output=output,
            error=None,
            metrics=TaskMetrics(duration_ms=max(0, int((time.time() - _START_TIME) * 1000))),
        )
    except Exception as exc:  # noqa: BLE001
        logger.exception("Task failed: %s", request.task_name)
        result = TaskResult(
            request_id=request.request_id,
            task_name=request.task_name,
            started_at=started_at,
            finished_at=datetime.now().astimezone().isoformat(),
            status=TaskStatus.ERROR,
            return_code=5,
            output=None,
            error=ErrorInfo(type=type(exc).__name__, message=str(exc), traceback=traceback.format_exc()),
            metrics=TaskMetrics(duration_ms=max(0, int((time.time() - _START_TIME) * 1000))),
        )

    atomic_write_json(result_path, redact_sensitive_data(result.model_dump(mode="json")))
    append_audit_record({"request": request.model_dump(mode="json"), "result": result.model_dump(mode="json")})
    return result.return_code


_START_TIME = time.time()


if __name__ == "__main__":
    sys.exit(main())
