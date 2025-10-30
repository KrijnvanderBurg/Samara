"""Test fixtures for the PySpark Ingestion Framework.

This module defines pytest fixtures that can be reused across test modules
to set up test environments, create test data, and manage resources.

Fixtures defined here are automatically available to all test modules in the project.
"""

import os
from collections.abc import Generator

import pytest
from pyspark.sql import SparkSession

# Set environment variable to suppress pyarrow timezone warning
# This is required for pyarrow>=2.0.0 with PySpark
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"


@pytest.fixture(scope="session")
def spark() -> Generator[SparkSession, None, None]:
    """Shared SparkSession for all unit tests.

    Using 'spark' as the fixture name to keep it concise in tests.
    Configured for minimal resource usage and fast test execution.
    """
    spark = (
        SparkSession.builder.appName("samara_unit_tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


pytest_plugins: list[str] = [
    # "tests.unit. ...
]
