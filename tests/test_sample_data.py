"""The sample generator must match the Monee export format and keep the messy cases."""
import csv
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

MONEE_HEADER = [
    "Account name", "Category", "Description", "Person", "Date", "Time", "Amount",
    "Recurring", "Payment status", "moneeTransactionID", "moneeCategoryID",
    "moneeAccountID", "moneePersonID", "moneeTimeZone", "moneeAssociatedAccountID",
    "moneeAssociatedAmount",
]
YEN = re.compile(r"^-?¥\d{1,3}(,\d{3})*$")              # "-¥1,940", "¥1,370"
MONEE_DATE = re.compile(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$")  # "August 31, 2026"


def _yen(s: str) -> int:
    return int(s.replace("¥", "").replace(",", ""))


def _generate(tmp_path):
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_sample_data.py"), "--out", str(tmp_path)],
        check=True,
    )
    files = [tmp_path / "person_a.csv", tmp_path / "person_b.csv"]
    headers = [next(csv.reader(f.open(encoding="utf-8"))) for f in files]
    a, b = (list(csv.DictReader(f.open(encoding="utf-8"))) for f in files)
    return headers, a, b


def test_generator_matches_monee_export_format(tmp_path):
    headers, a, b = _generate(tmp_path)
    assert headers == [MONEE_HEADER, MONEE_HEADER]
    rows = a + b
    assert all(YEN.match(r["Amount"]) for r in rows)
    assert any("," in r["Amount"] for r in rows)                  # thousands separator present
    assert all(re.match(r"^\d{2}:\d{2}:\d{2}$", r["Time"]) for r in rows)
    assert all(MONEE_DATE.match(r["Date"]) for r in a)


def test_generator_source_ids_unique_except_the_duplicate(tmp_path):
    _, a, b = _generate(tmp_path)
    ids = [r["moneeTransactionID"] for r in a + b if r["moneeTransactionID"]]
    assert len(ids) == len(set(ids)) + 1                          # only the exact duplicate repeats
    assert any(r["moneeTransactionID"] == "" for r in b)          # a row that needs the hash fallback


def test_generator_keeps_messy_edge_cases(tmp_path):
    _, a, b = _generate(tmp_path)
    assert any(_yen(r["Amount"]) > 0 for r in b)                  # a refund / credit
    assert any(r["Description"] == "" for r in a)                # empty description
    assert any(r["Category"] != r["Category"].strip() for r in a)  # untrimmed category
    assert any(not MONEE_DATE.match(r["Date"]) for r in b)        # unparseable date
    assert len({tuple(r.values()) for r in a}) < len(a)          # an exact duplicate
