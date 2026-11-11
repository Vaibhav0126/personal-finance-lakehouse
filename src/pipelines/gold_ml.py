"""
Gold ML: score every silver transaction with the champion category model.

Uses mlflow.sklearn.load_model + pandas_udf rather than pyfunc.spark_udf:
  * pandas_udf passes a pandas Series, which TfidfVectorizer iterates as
    strings. pyfunc passes a DataFrame, which iterates as column names and
    fails on .lower().
  * Plain sklearn pickles survive Python-version differences between the
    training runtime and the pipeline runtime.

Bootstrap-safe: on the very first run no model is registered yet, so
predictions are NULL instead of failing the whole pipeline. The daily job
trains and promotes a model right after the first refresh.
"""
import dlt
import pandas as pd
from pyspark.sql.functions import col, lit, pandas_udf, when
from pyspark.sql.types import StringType

MODEL_URI = spark.conf.get("pf.model_uri")  # models:/<catalog>.<schema>.<name>@champion

_cached_model = None
_model_missing = False

_NOT_FOUND_CODES = {"RESOURCE_DOES_NOT_EXIST", "NOT_FOUND"}


def _is_missing_model_error(exc) -> bool:
    """True only for 'model or alias does not exist'; anything else is a real failure."""
    if getattr(exc, "error_code", None) in _NOT_FOUND_CODES:
        return True
    msg = str(exc).lower()
    return "alias" in msg and ("not found" in msg or "does not exist" in msg)


def _get_model():
    """Load once per executor and cache. Returns None if no champion exists yet."""
    global _cached_model, _model_missing
    if _cached_model is None and not _model_missing:
        import mlflow
        import mlflow.sklearn
        from mlflow.exceptions import MlflowException

        # Executors don't inherit the driver's MLflow config: without both URIs
        # MLflow falls back to a local ./mlruns directory and crashes.
        mlflow.set_tracking_uri("databricks")
        mlflow.set_registry_uri("databricks-uc")
        try:
            _cached_model = mlflow.sklearn.load_model(MODEL_URI)
        except MlflowException as exc:  # RestException is a subclass
            if not _is_missing_model_error(exc):
                raise
            print(f"No champion model at {MODEL_URI}; predictions will be NULL. ({exc})")
            _model_missing = True
    return _cached_model


@pandas_udf(StringType())
def predict_category(description: pd.Series) -> pd.Series:
    model = _get_model()
    if model is None:
        return pd.Series([None] * len(description), dtype="object")
    return pd.Series(model.predict(description.fillna("").tolist()))


@dlt.table(
    name="gold_predicted_categories",
    comment="Every expense scored by the champion model: human label, prediction, mismatch flag.",
    table_properties={"quality": "gold"},
)
def gold_predicted_categories():
    return (
        dlt.read("silver_transactions")
        .where(col("direction") == "expense")
        .withColumn("predicted_category", predict_category(col("description")))
        .withColumn(
            "is_mismatch",
            when(col("predicted_category").isNull(), lit(None).cast("boolean"))
            .otherwise(col("category") != col("predicted_category")),
        )
        .select(
            "person", "txn_date", "year_month", "description", "amount",
            col("category").alias("human_category"), "predicted_category", "is_mismatch",
        )
    )


@dlt.table(
    name="gold_category_mismatches",
    comment="Rows where the model disagrees with the human label: either a mislabel or a model gap.",
    table_properties={"quality": "gold"},
)
def gold_category_mismatches():
    return (
        dlt.read("gold_predicted_categories")
        .where(col("is_mismatch") & col("description").isNotNull())
        .select("person", "txn_date", "description", "amount", "human_category", "predicted_category")
    )
