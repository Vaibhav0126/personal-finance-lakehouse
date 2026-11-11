"""
Train the transaction category classifier and promote it only if it beats the
current champion.

Runs as a serverless `spark_python_task` (see databricks.yml), after the
pipeline refresh in the same job:

  1. Load expense descriptions + human categories from silver.
  2. Train TF-IDF (1-2 grams) + logistic regression on an 80/20 split.
  3. Register the new version and tag it with the `challenger` alias.
  4. Score the current `champion` on the SAME test split.
  5. Move `champion` to the challenger only if promotion.should_promote() agrees.
"""
import argparse
import sys
from pathlib import Path

# Make src/ importable whether run on Databricks or locally.
sys.path.append(str(Path(__file__).resolve().parents[1]))

import mlflow  # noqa: E402
import mlflow.sklearn  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from sklearn.feature_extraction.text import TfidfVectorizer  # noqa: E402
from sklearn.linear_model import LogisticRegression  # noqa: E402
from sklearn.metrics import classification_report  # noqa: E402
from sklearn.model_selection import train_test_split  # noqa: E402
from sklearn.pipeline import Pipeline  # noqa: E402

from ml.promotion import should_promote  # noqa: E402

CHAMPION, CHALLENGER = "champion", "challenger"


def parse_args(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True)
    p.add_argument("--schema", required=True)
    p.add_argument("--model-name", required=True)
    p.add_argument("--experiment", required=True)
    p.add_argument("--min-improvement", type=float, default=0.0)
    p.add_argument("--test-size", type=float, default=0.2)
    return p.parse_args(argv)


def load_training_data(spark, catalog, schema):
    return (
        spark.table(f"{catalog}.{schema}.silver_transactions")
        .where("direction = 'expense' AND description IS NOT NULL AND category IS NOT NULL")
        .select("description", "category")
        .toPandas()
    )


def champion_accuracy(client, model_name, X_test, y_test):
    """Accuracy of the current champion on this run's test set, or None."""
    try:
        client.get_model_version_by_alias(model_name, CHAMPION)
    except Exception:
        return None
    champion = mlflow.sklearn.load_model(f"models:/{model_name}@{CHAMPION}")
    return float(champion.score(X_test, y_test))


def main(argv=None):
    args = parse_args(argv)
    spark = SparkSession.builder.getOrCreate()
    model_name = f"{args.catalog}.{args.schema}.{args.model_name}"

    mlflow.set_tracking_uri("databricks")
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(args.experiment)
    client = MlflowClient()

    df = load_training_data(spark, args.catalog, args.schema)
    print(f"Training rows: {len(df)}\n{df['category'].value_counts()}")
    if df["category"].nunique() < 2:
        print("Need at least two categories to train; skipping.")
        return

    X, y = df["description"], df["category"]
    stratify = y if y.value_counts().min() >= 2 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=stratify
    )

    pipeline = Pipeline([
        ("tfidf", TfidfVectorizer(ngram_range=(1, 2), lowercase=True)),
        ("clf", LogisticRegression(max_iter=1000, class_weight="balanced")),
    ])

    with mlflow.start_run(run_name="tfidf_logreg") as run:
        pipeline.fit(X_train, y_train)
        test_acc = float(pipeline.score(X_test, y_test))

        mlflow.log_params({
            "vectorizer": "tfidf_1_2_gram",
            "classifier": "logistic_regression",
            "class_weight": "balanced",
            "train_rows": len(X_train),
            "test_rows": len(X_test),
            "n_classes": int(y.nunique()),
        })
        mlflow.log_metric("train_accuracy", float(pipeline.score(X_train, y_train)))
        mlflow.log_metric("test_accuracy", test_acc)
        mlflow.log_text(
            classification_report(y_test, pipeline.predict(X_test), zero_division=0),
            "classification_report.txt",
        )

        # Unity Catalog requires a signature.
        signature = mlflow.models.infer_signature(
            X_train.to_list(), pipeline.predict(X_train[:5])
        )
        info = mlflow.sklearn.log_model(
            sk_model=pipeline,
            artifact_path="model",
            signature=signature,
            registered_model_name=model_name,
        )
        new_version = info.registered_model_version
        client.set_registered_model_alias(model_name, CHALLENGER, new_version)

        # Compare on the same test split, not against the champion's old metric.
        champ_acc = champion_accuracy(client, model_name, X_test, y_test)
        decision = should_promote(test_acc, champ_acc, args.min_improvement)

        mlflow.log_metric("champion_accuracy_same_split", champ_acc if champ_acc is not None else -1.0)
        mlflow.set_tags({"promoted": str(decision.promote), "promotion_reason": decision.reason})

        if decision.promote:
            client.set_registered_model_alias(model_name, CHAMPION, new_version)
        print(
            f"Version {new_version}: test accuracy {test_acc:.2%}. "
            f"{'PROMOTED' if decision.promote else 'kept as challenger'} - {decision.reason}. "
            f"Run {run.info.run_id}"
        )


if __name__ == "__main__":
    main()
