"""Gold: business-level tables for the dashboard."""
import sys

sys.path.append(spark.conf.get("bundle.sourcePath", "."))

import dlt  # noqa: E402
from pyspark.sql.functions import col, count, sum as _sum, trunc  # noqa: E402
from transforms.matching import match_shared_expenses, unmatched_expenses  # noqa: E402

MATCH_DAYS = int(spark.conf.get("pf.shared_match_days", "3"))


@dlt.table(
    name="gold_monthly_spend",
    comment="Q: How much did each person spend per month and category?",
    table_properties={"quality": "gold"},
)
def gold_monthly_spend():
    return (
        dlt.read("silver_transactions")
        .where(col("direction") == "expense")
        .groupBy("person", "year_month", "category")
        .agg(_sum("amount").alias("total_amount"), count("*").alias("txn_count"))
    )


@dlt.table(
    name="gold_credits",
    comment="Q: Which refunds or incoming payments were recorded?",
    table_properties={"quality": "gold"},
)
def gold_credits():
    return (
        dlt.read("silver_transactions")
        .where(col("direction") == "credit")
        .select("person", "txn_date", "category", "description", "amount")
    )


@dlt.table(
    name="gold_shared_expenses",
    comment=f"Q: Which purchases were split? Same amount and category, logged by both people within {MATCH_DAYS} days.",
    table_properties={"quality": "gold"},
)
def gold_shared_expenses():
    return match_shared_expenses(dlt.read("silver_transactions"), max_days=MATCH_DAYS)


@dlt.table(
    name="gold_unshared_expenses",
    comment="Q: Which expenses did only one person log? Review to catch items the other forgot.",
    table_properties={"quality": "gold"},
)
def gold_unshared_expenses():
    return (
        unmatched_expenses(dlt.read("silver_transactions"), dlt.read("gold_shared_expenses"))
        .select("person", "txn_date", "category", "description", "amount")
    )


@dlt.table(
    name="gold_household_spend",
    comment="Q: What does the household spend together each month (shared items only)?",
    table_properties={"quality": "gold"},
)
def gold_household_spend():
    return (
        dlt.read("gold_shared_expenses")
        .withColumn("year_month", trunc(col("latest_logged"), "month"))
        .groupBy("year_month", "category")
        .agg(_sum("full_cost").alias("household_total"), count("*").alias("shared_item_count"))
    )
