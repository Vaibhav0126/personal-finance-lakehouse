import pytest


@pytest.fixture(scope="session")
def spark():
    """Local Spark with ANSI mode on, matching Databricks defaults."""
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder
        .master("local[1]")
        .appName("personal-finance-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
