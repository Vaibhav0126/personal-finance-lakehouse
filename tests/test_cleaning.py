"""Tests for transforms/cleaning.py: the exact functions silver.py uses."""
import pytest

pytest.importorskip("pyspark")  # Spark tests skip cleanly where PySpark is not installed

from datetime import date
from decimal import Decimal

from transforms.cleaning import (
    add_direction,
    add_txn_id,
    cast_amount,
    clean_transactions,
    normalize_category,
    normalize_description,
    parse_txn_date,
)


def test_parse_txn_date_person_a_format(spark):
    df = spark.createDataFrame([("March 30, 2026",)], ["date_raw"])
    assert parse_txn_date(df).first().txn_date == date(2026, 3, 30)


def test_parse_txn_date_person_b_format(spark):
    df = spark.createDataFrame([("30 March 2026",)], ["date_raw"])
    assert parse_txn_date(df).first().txn_date == date(2026, 3, 30)


def test_parse_txn_date_bad_input_is_null_not_error(spark):
    # Under ANSI mode plain to_date would raise here; try_to_date returns NULL.
    df = spark.createDataFrame([("not a date",), (None,)], "date_raw string")
    assert [r.txn_date for r in parse_txn_date(df).collect()] == [None, None]


def test_normalize_category_trims_and_titlecases(spark):
    df = spark.createDataFrame([("  food  ",), ("BEVERAGES",), ("body care",)], ["category"])
    assert [r.category for r in normalize_category(df).collect()] == ["Food", "Beverages", "Body Care"]


def test_normalize_description_empty_becomes_null(spark):
    df = spark.createDataFrame([("  coffee ",), ("",), ("   ",), ("McD",)], ["description"])
    assert [r.description for r in normalize_description(df).collect()] == ["coffee", None, None, "McD"]


def test_cast_amount_bad_value_is_null(spark):
    df = spark.createDataFrame([("-650.00",), ("abc",)], ["amount"])
    assert [r.amount for r in cast_amount(df).collect()] == [Decimal("-650.00"), None]


def test_cast_amount_handles_monee_yen_format(spark):
    df = spark.createDataFrame([("-¥1,940",), ("¥1,370",), ("abc",)], ["amount"])
    assert [r.amount for r in cast_amount(df).collect()] == [Decimal("-1940.00"), Decimal("1370.00"), None]


def test_add_direction_keeps_refunds_as_credit(spark):
    df = spark.createDataFrame([(-100.0,), (5500.0,), (0.0,)], ["amount"])
    assert [r.direction for r in add_direction(df).collect()] == ["expense", "credit", "credit"]


def test_txn_id_is_deterministic_and_distinguishes_rows(spark):
    rows = [("Person A", date(2026, 3, 1), "Coffee", Decimal("-400.00"))] * 2 + [
        ("Person B", date(2026, 3, 1), "Coffee", Decimal("-400.00"))
    ]
    df = spark.createDataFrame(rows, "person string, txn_date date, description string, amount decimal(12,2)")
    ids = [r.txn_id for r in add_txn_id(df).collect()]
    assert ids[0] == ids[1] and ids[0] != ids[2]


_WITH_SOURCE_ID = "person string, txn_date date, description string, amount decimal(12,2), source_txn_id string"


def test_txn_id_keeps_identical_purchases_with_different_source_ids_separate(spark):
    purchase = ("Person A", date(2026, 8, 31), "Coffee", Decimal("-400.00"))
    df = spark.createDataFrame([purchase + ("MONEE-1",), purchase + ("MONEE-2",)], _WITH_SOURCE_ID)
    ids = [r.txn_id for r in add_txn_id(df).collect()]
    assert ids == ["MONEE-1", "MONEE-2"]


def test_txn_id_falls_back_to_hash_when_source_id_missing(spark):
    purchase = ("Person A", date(2026, 8, 31), "Coffee", Decimal("-400.00"))
    df = spark.createDataFrame([purchase + (None,), purchase + ("",), purchase + ("MONEE-1",)], _WITH_SOURCE_ID)
    ids = [r.txn_id for r in add_txn_id(df).collect()]
    assert ids[0] == ids[1] and len(ids[0]) == 64 and ids[2] == "MONEE-1"


def test_clean_transactions_end_to_end(spark):
    df = spark.createDataFrame(
        [("Person A", "March 30, 2026", "  food ", " Onigiri ", "-180.00")],
        ["person", "date_raw", "category", "description", "amount"],
    )
    r = clean_transactions(df).first()
    assert (r.txn_date, r.category, r.description, r.direction, r.year_month) == (
        date(2026, 3, 30), "Food", "Onigiri", "expense", date(2026, 3, 1)
    )
    assert r.txn_id is not None
