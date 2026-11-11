"""Bronze: raw CSV exports, ingested incrementally with Auto Loader. No cleaning."""
import dlt

LANDING_PATH = spark.conf.get("pf.landing_path")  # set in databricks.yml


@dlt.table(
    name="bronze_transactions",
    comment="Raw transactions from CSV uploads, one row per source line.",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.managed": "true"},
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "false")  # keep raw strings; silver casts
        .option("header", "true")
        .load(LANDING_PATH)
        # Explicit rename: easier to review than a dynamic loop.
        # Backticks handle source columns that contain spaces.
        .selectExpr(
            "`Account name` as account_name",
            "Category as category",
            "Description as description",
            "Person as person",
            "Date as date_raw",
            "Time as txn_time",
            "Amount as amount",
            "Recurring as recurring",
            "`Payment status` as payment_status",
            "moneeTransactionID as source_txn_id",
            "_metadata.file_path as _source_file",
            "current_timestamp() as _ingested_at",
        )
    )
