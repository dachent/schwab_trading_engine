import shutil
import uuid
from pathlib import Path

from imports import create_import_template, parse_import_workbook


def test_parse_import_workbook_round_trip() -> None:
    temp_root = Path(__file__).resolve().parent / ".tmp" / uuid.uuid4().hex
    temp_root.mkdir(parents=True, exist_ok=False)
    path = temp_root / "template.xlsx"
    create_import_template(path)

    try:
        rows, errors = parse_import_workbook(path)

        assert not errors
        assert len(rows) == 1
        assert rows[0].account_number == "12345678"
        assert rows[0].symbol == "SPY"
        assert rows[0].quantity == 10
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)
