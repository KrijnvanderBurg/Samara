"""E2E test for job execution via CLI with output verification."""

import glob
import json
import subprocess
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.testing import assertDataFrameEqual


@pytest.mark.parametrize("job_path", glob.glob("tests/e2e/workflow/**/job.json", recursive=True))
def test_job_command__execute_and_verify__matches_expected_output(
    tmp_path: Path, spark: SparkSession, job_path: str
) -> None:
    """Test job execution produces expected outputs.

    This test:
    1. Loads job config and redirects output paths to tmp directory
    2. Executes job via CLI subprocess (production-like with coverage)
    3. Verifies output data and schema match expected results

    Args:
        tmp_path: Pytest temporary directory
        spark: Spark session fixture
        job_path: Path to job.json configuration file
    """
    job_file = Path(job_path)
    job_dir = job_file.parent

    # Load and modify config to redirect outputs to tmp
    with open(job_file, encoding="utf-8") as f:
        config = json.load(f)

    for job in config["workflow"]["jobs"]:
        for load in job["loads"]:
            load["location"] = str(tmp_path / "outputs" / Path(load["location"]).name)
            load["schema_export"] = str(tmp_path / "outputs" / Path(load["schema_export"]).name)

    # Write modified config to tmp
    isolated_config_path = tmp_path / "isolated_job.json"
    with open(isolated_config_path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2)

    # Execute via CLI with coverage tracking
    _ = subprocess.run(
        [
            "coverage",
            "run",
            "--parallel-mode",
            "--branch",
            "--source=samara",
            "-m",
            "samara",
            "run",
            "--workflow-filepath",
            str(isolated_config_path),
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=120,
    )

    # Verify all outputs match expected results
    for job in config["workflow"]["jobs"]:
        for load in job["loads"]:
            expected_dir = job_dir / load["id"]
            actual_data_path = Path(load["location"])
            actual_schema_path = Path(load["schema_export"])

            # Find expected files
            expected_schema_path = expected_dir / "expected_schema.json"
            expected_data_files = list(expected_dir.glob("expected_output.*"))

            if not expected_data_files:
                continue

            expected_data_path = expected_data_files[0]
            data_format = expected_data_path.suffix.lstrip(".")

            # Verify schema
            if expected_schema_path.exists():
                with open(actual_schema_path, encoding="utf-8") as f:
                    actual_schema = StructType.fromJson(json.load(f))
                with open(expected_schema_path, encoding="utf-8") as f:
                    expected_schema = StructType.fromJson(json.load(f))
                assert actual_schema == expected_schema

            # Verify data
            with open(actual_schema_path, encoding="utf-8") as f:
                schema = StructType.fromJson(json.load(f))

            df_actual = spark.read.format(data_format).schema(schema).load(str(actual_data_path))
            df_expected = spark.read.format(data_format).schema(schema).load(str(expected_data_path))
            assertDataFrameEqual(actual=df_actual, expected=df_expected)
