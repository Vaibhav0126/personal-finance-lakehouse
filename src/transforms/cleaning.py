"""
Pure transformation functions used by the silver layer.

The DLT pipeline imports these exact functions, so the unit tests in
tests/test_cleaning.py exercise the same code that runs in production.

Rule: every function takes a DataFrame and returns a DataFrame.
No I/O, no DLT decorators, no notebook magic.
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce, col, concat_ws, expr, initcap, lit, sha2, trim, trunc, when,
)

# Source exports arrive in two date styles, one per person:
#   Person A: "March 30, 2026"
#   Person B: "30 March 2026"
_DATE_FORMATS = ("MMMM d yyyy", "d MMMM yyyy")


def parse_txn_date(
    df: DataFrame,
    src_col: str = "date_raw",
    dst_col: str = "txn_date",
) -> DataFrame:
    """Parse both source date styles into one DATE column.

    Commas are stripped first so only space-separated forms need parsing.
    try_to_date turns unparseable values into NULL instead of failing the
    pipeline under ANSI mode; NULL dates are then routed to quarantine.
    """
    normalized = f"regexp_replace(`{src_col}`, ',', '')"
    return df.withColumn(
        dst_col,
        coalesce(*[expr(f"try_to_date({normalized}, '{fmt}')") for fmt in _DATE_FORMATS]),
    )


def normalize_category(df: DataFrame, col_name: str = "category") -> DataFrame:
    """Trim whitespace and title-case category values ('  food ' -> 'Food')."""
    return df.withColumn(col_name, initcap(trim(col(col_name))))


def normalize_description(df: DataFrame, col_name: str = "description") -> DataFrame:
    """Trim whitespace; empty strings become NULL so data-quality checks see them."""
    cleaned = trim(col(col_name))
    return df.withColumn(col_name, when(cleaned == "", lit(None)).otherwise(cleaned))


def cast_amount(df: DataFrame, col_name: str = "amount") -> DataFrame:
    """Cast to DECIMAL(12,2); unparseable amounts become NULL (-> quarantine)."""
    return df.withColumn(col_name, expr(f"try_cast(`{col_name}` AS DECIMAL(12,2))"))


def add_direction(df: DataFrame, amount_col: str = "amount") -> DataFrame:
    """Label each row 'expense' (negative) or 'credit' (refunds, income, zero).

    Credits are kept rather than dropped, so refunds are visible downstream
    instead of silently disappearing.
    """
    return df.withColumn(
        "direction",
        when(col(amount_col) < 0, lit("expense")).otherwise(lit("credit")),
    )


def add_txn_id(df: DataFrame) -> DataFrame:
    """Deterministic id per transaction, used for de-duplication and matching."""
    key = concat_ws(
        "|",
        coalesce(col("person"), lit("")),
        coalesce(col("txn_date").cast("string"), lit("")),
        coalesce(col("description"), lit("")),
        coalesce(col("amount").cast("string"), lit("")),
    )
    return df.withColumn("txn_id", sha2(key, 256))


def clean_transactions(df: DataFrame) -> DataFrame:
    """Full silver cleaning step, applied to the renamed bronze columns."""
    out = parse_txn_date(df)
    out = normalize_description(out)
    out = normalize_category(out)
    out = cast_amount(out)
    out = add_direction(out)
    out = add_txn_id(out)
    return out.withColumn("year_month", trunc(col("txn_date"), "month"))
