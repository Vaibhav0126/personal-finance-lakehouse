"""
Shared-expense matching.

Both people log their own half of a shared purchase (rent, groceries, a
dinner). A pair counts as shared when two different people log an expense
with the same amount and category within `max_days` of each other.

Earlier version matched on amount + category + same month, which paired
unrelated purchases (two different coffees at the same price) and produced
duplicate rows when an amount repeated. This version:
  * uses a day window instead of a calendar month, and
  * keeps only mutual-best matches, so each transaction appears in at most
    one pair (no fan-out).

It is still a heuristic. If exports ever gain an explicit "shared" flag,
prefer that over inference.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import abs as _abs, col, datediff, greatest, row_number


def match_shared_expenses(silver: DataFrame, max_days: int = 3) -> DataFrame:
    expenses = silver.where(col("direction") == "expense")
    a = expenses.alias("a")
    b = expenses.alias("b")

    candidates = (
        a.join(
            b,
            (col("a.amount") == col("b.amount"))
            & (col("a.category") == col("b.category"))
            & (col("a.person") < col("b.person"))
            & (_abs(datediff(col("a.txn_date"), col("b.txn_date"))) <= max_days),
            "inner",
        )
        .select(
            col("a.txn_id").alias("txn_id_a"),
            col("b.txn_id").alias("txn_id_b"),
            col("a.person").alias("person_a"),
            col("b.person").alias("person_b"),
            col("a.txn_date").alias("txn_date_a"),
            col("b.txn_date").alias("txn_date_b"),
            col("a.category").alias("category"),
            col("a.amount").alias("each_half"),
            col("a.description").alias("description_a"),
            col("b.description").alias("description_b"),
            _abs(datediff(col("a.txn_date"), col("b.txn_date"))).alias("day_gap"),
        )
    )

    # Closest partner first; txn ids break ties so results are deterministic.
    best_for_a = Window.partitionBy("txn_id_a").orderBy("day_gap", "txn_id_b")
    best_for_b = Window.partitionBy("txn_id_b").orderBy("day_gap", "txn_id_a")

    return (
        candidates
        .withColumn("rank_a", row_number().over(best_for_a))
        .withColumn("rank_b", row_number().over(best_for_b))
        .where((col("rank_a") == 1) & (col("rank_b") == 1))
        .drop("rank_a", "rank_b")
        .withColumn("full_cost", col("each_half") * 2)
        .withColumn("latest_logged", greatest(col("txn_date_a"), col("txn_date_b")))
    )


def unmatched_expenses(silver: DataFrame, shared: DataFrame) -> DataFrame:
    """Expenses that are not part of any shared pair."""
    matched_ids = (
        shared.select(col("txn_id_a").alias("txn_id"))
        .union(shared.select(col("txn_id_b").alias("txn_id")))
    )
    return (
        silver.where(col("direction") == "expense")
        .join(matched_ids, "txn_id", "left_anti")
    )
