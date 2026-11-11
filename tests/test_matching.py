"""Tests for transforms/matching.py (shared-expense detection)."""
import pytest

pytest.importorskip("pyspark")  # Spark tests skip cleanly where PySpark is not installed

from datetime import date
from decimal import Decimal

from transforms.matching import match_shared_expenses, unmatched_expenses

SCHEMA = "txn_id string, person string, txn_date date, category string, amount decimal(12,2), description string, direction string"


def _df(spark, rows):
    return spark.createDataFrame(
        [(i, p, d, c, Decimal(a), desc, "expense" if Decimal(a) < 0 else "credit")
         for i, p, d, c, a, desc in rows],
        SCHEMA,
    )


def test_pairs_same_amount_category_within_window(spark):
    silver = _df(spark, [
        ("a1", "Person A", date(2026, 3, 1), "Food", "-2500", "Groceries"),
        ("b1", "Person B", date(2026, 3, 2), "Food", "-2500", "Groceries"),
    ])
    shared = match_shared_expenses(silver, max_days=3).collect()
    assert [(r.txn_id_a, r.txn_id_b, r.full_cost) for r in shared] == [("a1", "b1", Decimal("-5000.00"))]


def test_same_month_but_outside_window_is_not_shared(spark):
    # The old month-based rule would have paired these two coffees.
    silver = _df(spark, [
        ("a1", "Person A", date(2026, 3, 1), "Beverages", "-450", "Coffee"),
        ("b1", "Person B", date(2026, 3, 25), "Beverages", "-450", "Coffee"),
    ])
    assert match_shared_expenses(silver, max_days=3).count() == 0


def test_each_transaction_used_at_most_once(spark):
    # Two identical rent halves from A, one from B: only one pair, closest date wins.
    silver = _df(spark, [
        ("a1", "Person A", date(2026, 3, 1), "Housing", "-22500", "Rent"),
        ("a2", "Person A", date(2026, 3, 3), "Housing", "-22500", "Rent"),
        ("b1", "Person B", date(2026, 3, 3), "Housing", "-22500", "Rent"),
    ])
    shared = match_shared_expenses(silver, max_days=3).collect()
    assert [(r.txn_id_a, r.txn_id_b) for r in shared] == [("a2", "b1")]


def test_credits_are_never_shared(spark):
    silver = _df(spark, [
        ("a1", "Person A", date(2026, 3, 1), "Leisure", "5500", "Refund"),
        ("b1", "Person B", date(2026, 3, 1), "Leisure", "5500", "Refund"),
    ])
    assert match_shared_expenses(silver).count() == 0


def test_unmatched_returns_only_unpaired_expenses(spark):
    silver = _df(spark, [
        ("a1", "Person A", date(2026, 3, 1), "Food", "-2500", "Groceries"),
        ("b1", "Person B", date(2026, 3, 2), "Food", "-2500", "Groceries"),
        ("a2", "Person A", date(2026, 3, 5), "Gift", "-3000", "Birthday gift"),
        ("b2", "Person B", date(2026, 3, 6), "Leisure", "5500", "Refund"),
    ])
    shared = match_shared_expenses(silver)
    assert [r.txn_id for r in unmatched_expenses(silver, shared).collect()] == ["a2"]
