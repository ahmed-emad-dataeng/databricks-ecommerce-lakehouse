"""Shared Spark fixture.

On Databricks the ambient `spark` session is reused. Locally a session is
created, which needs a JDK -- see README ("Running the tests").
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    try:  # Databricks: reuse the session the runtime already gave us.
        from databricks.sdk.runtime import spark as dbx_spark

        return dbx_spark
    except ImportError:
        pass

    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[2]")
        .appName("ecommerce-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
