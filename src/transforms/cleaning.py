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

# Monee exports use "March 30, 2026"; "30 March 2026" is also accepted for robustness.
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
    """Cast to DECIMAL(12,2); unparseable amounts become NULL (-> quarantine).

    Monee exports amounts like "-¥1,940", so everything except digits, '-' and
    '.' is stripped first.
    """
    digits = f"regexp_replace(`{col_name}`, '[^0-9.-]', '')"
    return df.withColumn(col_name, expr(f"try_cast({digits} AS DECIMAL(12,2))"))


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
    """Id per transaction, used for de-duplication and matching.

    Prefers the source app's id (source_txn_id), so two genuinely identical
    purchases stay separate. Falls back to a content hash when it is missing.
    """
    key = concat_ws(
        "|",
        coalesce(col("person"), lit("")),
        coalesce(col("txn_date").cast("string"), lit("")),
        coalesce(col("description"), lit("")),
        coalesce(col("amount").cast("string"), lit("")),
    )
    hashed = sha2(key, 256)
    if "source_txn_id" not in df.columns:
        return df.withColumn("txn_id", hashed)
    source_id = when(trim(col("source_txn_id")) == "", lit(None)).otherwise(trim(col("source_txn_id")))
    return df.withColumn("txn_id", coalesce(source_id, hashed))


def clean_transactions(df: DataFrame) -> DataFrame:
    """Full silver cleaning step, applied to the renamed bronze columns."""
    out = parse_txn_date(df)
    out = normalize_description(out)
    out = normalize_category(out)
    out = cast_amount(out)
    out = add_direction(out)
    out = add_txn_id(out)
    return out.withColumn("year_month", trunc(col("txn_date"), "month"))
