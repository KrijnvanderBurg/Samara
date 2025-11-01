"""Result verification utilities for E2E tests."""

import json
import logging
from pathlib import Path

from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType
from pyspark.testing import assertDataFrameEqual
from samara.workflow.jobs.spark.session import SparkHandler

logger = logging.getLogger(__name__)


class ResultVerifier:
    """Verifies test execution results against expected outputs."""

    def __init__(self, job_dir: Path) -> None:
        """Initialize verifier with job directory.

        Args:
            job_dir: Directory containing job.json and expected outputs
        """
        self.job_dir = job_dir
        self.spark: SparkSession = SparkHandler().session

    def verify_outputs(self, isolated_config: dict) -> None:
        """Verify all outputs match expected results.

        Args:
            isolated_config: Job configuration with tmp output paths
        """
        for job in isolated_config["workflow"]["jobs"]:
            for load in job["loads"]:
                expected_dir = self.job_dir / load["id"]

                self._verify_single_output(
                    load_name=load["id"],
                    actual_data_path=Path(load["location"]),
                    actual_schema_path=Path(load["schema_export"]),
                    expected_dir=expected_dir,
                )

    def _verify_single_output(
        self, load_name: str, actual_data_path: Path, actual_schema_path: Path, expected_dir: Path
    ) -> None:
        """Verify a single output against expected results.

        Args:
            load_name: Name of the load output
            actual_data_path: Path to actual output data
            actual_schema_path: Path to actual output schema
            expected_dir: Directory containing expected outputs
        """
        # Find expected files
        expected_schema_path = expected_dir / "expected_schema.json"
        expected_data_files = list(expected_dir.glob("expected_output.*"))

        if not expected_data_files:
            logger.warning("No expected data file found for load '%s'", load_name)
            return

        expected_data_path = expected_data_files[0]
        data_format = expected_data_path.suffix.lstrip(".")

        # Verify schema
        if expected_schema_path.exists():
            self._verify_schema(actual_schema_path, expected_schema_path)

        # Verify data
        self._verify_data(
            actual_data_path=actual_data_path,
            expected_data_path=expected_data_path,
            data_format=data_format,
            schema_path=actual_schema_path,
        )

    def _verify_schema(self, actual_path: Path, expected_path: Path) -> None:
        """Compare actual and expected schemas.

        Args:
            actual_path: Path to actual schema
            expected_path: Path to expected schema
        """
        with open(actual_path, encoding="utf-8") as f:
            actual_schema = StructType.fromJson(json.load(f))

        with open(expected_path, encoding="utf-8") as f:
            expected_schema = StructType.fromJson(json.load(f))

        assert actual_schema == expected_schema, f"Schema mismatch: {actual_schema} != {expected_schema}"

    def _verify_data(
        self, actual_data_path: Path, expected_data_path: Path, data_format: str, schema_path: Path
    ) -> None:
        """Compare actual and expected data.

        Args:
            actual_data_path: Path to actual data
            expected_data_path: Path to expected data
            data_format: Data format (e.g., 'csv', 'json')
            schema_path: Path to schema file
        """
        # Load schema
        with open(schema_path, encoding="utf-8") as f:
            schema = StructType.fromJson(json.load(f))

        # Read actual data
        df_actual = self.spark.read.format(data_format).schema(schema).load(str(actual_data_path))

        # Read expected data
        df_expected = self.spark.read.format(data_format).schema(schema).load(str(expected_data_path))

        # Compare DataFrames
        assertDataFrameEqual(actual=df_actual, expected=df_expected)
