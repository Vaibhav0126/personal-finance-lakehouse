"""The sample generator must keep producing the messy cases the pipeline handles."""
import csv
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_generator_writes_both_date_styles_and_edge_cases(tmp_path):
    subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_sample_data.py"), "--out", str(tmp_path)],
        check=True,
    )
    a = list(csv.DictReader((tmp_path / "person_a.csv").open()))
    b = list(csv.DictReader((tmp_path / "person_b.csv").open()))

    assert a and b
    assert "," in a[0]["Date"] and "," not in b[0]["Date"]       # "March 30, 2026" vs "30 March 2026"
    assert any(float(r["Amount"]) > 0 for r in b)                # a refund / credit
    assert any(r["Description"] == "" for r in a)                # empty description
    assert any(not r["Date"][0].isdigit() and "," not in r["Date"] for r in b)  # unparseable date
    assert len({tuple(r.values()) for r in a}) < len(a)          # an exact duplicate
