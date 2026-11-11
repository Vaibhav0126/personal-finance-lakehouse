"""
Silver: cleaned, typed, de-duplicated transactions.

All cleaning logic lives in src/transforms/cleaning.py and is unit-tested;
this file only wires it into DLT and declares the data-quality rules.
Rows with an unparseable date or amount go to silver_quarantine instead of
being silently dropped.
"""
import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))

import dlt  # noqa: E402
from transforms.cleaning import clean_transactions  # noqa: E402

KNOWN_CATEGORIES = (
    "Food", "Beverages", "Housing", "Gift", "Body Care", "Leisure", "Vacation",
)
_known_sql = ", ".join(f"'{c}'" for c in KNOWN_CATEGORIES)

SILVER_COLUMNS = [
    "txn_id", "account_name", "person", "txn_date", "year_month", "category",
    "description", "amount", "direction", "recurring", "payment_status",
    "_source_file", "_ingested_at",
]


@dlt.view(name="transactions_cleaned", comment="Bronze rows after the shared cleaning step.")
def transactions_cleaned():
    return clean_transactions(dlt.read_stream("bronze_transactions"))


@dlt.table(
    name="silver_transactions",
    comment="Cleaned, typed, de-duplicated transactions (expenses and credits).",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_date", "txn_date IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount IS NOT NULL")
@dlt.expect("has_description", "description IS NOT NULL")
@dlt.expect("known_category", f"category IN ({_known_sql})")
def silver_transactions():
    return (
        dlt.read_stream("transactions_cleaned")
        .dropDuplicates(["txn_id"])
        .select(*SILVER_COLUMNS)
    )


@dlt.table(
    name="silver_quarantine",
    comment="Rows rejected from silver (bad date or amount), kept for review.",
    table_properties={"quality": "quarantine"},
)
def silver_quarantine():
    return (
        dlt.read_stream("transactions_cleaned")
        .where("txn_date IS NULL OR amount IS NULL")
    )
