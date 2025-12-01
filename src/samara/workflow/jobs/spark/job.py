"""Spark job - Execute ETL pipelines using Apache Spark.

This module provides the JobSpark class, which orchestrates Extract, Transform,
and Load operations using Spark as the processing engine. It executes configuration-
driven pipelines in sequence, managing data flow from sources through transformations
to destinations with automatic component instantiation.
"""

import logging
import time

from typing_extensions import override

from samara.telemetry import trace_span
from samara.types import DataFrameRegistry, StreamingQueryRegistry
from samara.utils.logger import get_logger
from samara.workflow.jobs.models.model_job import JobEngine, JobModel
from samara.workflow.jobs.spark.extract import ExtractSparkUnion
from samara.workflow.jobs.spark.load import LoadSparkUnion
from samara.workflow.jobs.spark.transform import TransformSparkUnion

logger: logging.Logger = get_logger(__name__)


class JobSpark(JobModel[ExtractSparkUnion, TransformSparkUnion, LoadSparkUnion]):
    """Execute an ETL job orchestrating extract, transform, and load operations.

    JobSpark is the main entry point for Spark-based ETL pipelines. It coordinates
    the sequential execution of extraction, transformation, and loading operations
    defined in a configuration-driven pipeline. The class manages the complete
    lifecycle of a pipeline from data ingestion to final output.

    Attributes:
        id (str): Unique identifier for this ETL job.
        extracts (list[ExtractSparkUnion]): Collection of Extract components that
            retrieve data from configured sources.
        transforms (list[TransformSparkUnion]): Collection of Transform components
            that apply operations (select, filter, join, cast) to modify data.
        loads (list[LoadSparkUnion]): Collection of Load components that write
            transformed data to target destinations.
        engine_type (JobEngine): Engine type identifier set to SPARK.

    Example:
        **Configuration in JSON:**
        ```
        {
            "id": "customer_etl",
            "extracts": [
                {
                    "id": "load_customers",
                    "type": "csv",
                    "path": "data/customers.csv"
                    ...
                }
            ],
            "transforms": [
                {
                    "id": "filter_active",
                    "type": "filter",
                    "condition": "status = 'active'"
                    ...
                }
            ],
            "loads": [
                {
                    "id": "output_customers",
                    "type": "csv",
                    "path": "output/active_customers.csv"
                    ...
                }
            ]
        }
        ```

        **Configuration in YAML:**
        ```
        id: customer_etl
        extracts:
          - id: load_customers
            type: csv
            path: data/customers.csv
            ...
        transforms:
          - id: filter_active
            type: filter
            condition: "status = 'active'"
            ...
        loads:
          - id: output_customers
            type: csv
            path: output/active_customers.csv
            ...
        ```

        **Usage:**
        ```python
        from pathlib import Path
        from samara.workflow.jobs.spark.job import JobSpark

        job = JobSpark.from_file(Path("config.json"))
        job.execute()
        ```

    Note:
        Private methods (_extract, _transform, _load) are executed sequentially
        to ensure data flows correctly through the pipeline. The registries are
        cleared after execution to prevent memory leaks across jobs.
    """

    engine_type: JobEngine = JobEngine.SPARK

    @override
    @trace_span("spark_job._execute")
    def _execute(self) -> None:
        """Execute the ETL pipeline through extract, transform, and load phases.

        Orchestrates the complete pipeline by sequentially executing the extract,
        transform, and load phases. Each phase processes all configured components
        in order, with execution timing logged for monitoring and debugging purposes.

        Raises:
            Exception: If any extract, transform, or load operation fails during
                execution. The pipeline stops at the first failure point.

        Note:
            Execution time is measured and logged for performance monitoring.
            Use logger with level DEBUG to see detailed timing per component.
        """
        start_time = time.time()
        logger.info(
            "Starting Spark job execution with %d extracts, %d transforms, %d loads",
            len(self.extracts),
            len(self.transforms),
            len(self.loads),
        )

        self._extract()
        self._transform()
        self._load()

        execution_time = time.time() - start_time
        logger.info("Spark job completed in %.2f seconds", execution_time)

    @trace_span("spark_job._extract")
    def _extract(self) -> None:
        """Extract data from all configured sources.

        Executes each extract component sequentially, retrieving data from sources
        (CSV, JSON, databases, etc.) and populating the DataFrameRegistry. All
        extracted data is available to downstream transform components.

        Raises:
            Exception: If any extract operation fails. The phase stops at the
                first failure without executing subsequent extractors.

        Note:
            Extraction is always the first phase and must succeed before transforms
            can execute. Enable DEBUG logging to see individual extractor timing.
        """
        logger.info("Starting extract phase with %d extractors", len(self.extracts))
        start_time = time.time()

        for i, extract in enumerate(self.extracts):
            extract_start_time = time.time()
            logger.debug("Running extractor %d/%d: %s", i, len(self.extracts), extract.id_)
            extract.extract()
            extract_time = time.time() - extract_start_time
            logger.debug("Extractor %s completed in %.2f seconds", extract.id_, extract_time)

        phase_time = time.time() - start_time
        logger.info("Extract phase completed successfully in %.2f seconds", phase_time)

    @trace_span("spark_job._transform")
    def _transform(self) -> None:
        """Apply transformation operations to extracted data.

        Executes each transform component sequentially, applying operations like
        select, filter, join, and cast to modify and enrich data. Each transform
        reads from the DataFrameRegistry and writes results back for use by
        subsequent transforms or load components.

        Raises:
            Exception: If any transform operation fails. The phase stops at the
                first failure without executing subsequent transformers.

        Note:
            Transform dependencies are resolved by component ordering in the
            configuration. Enable DEBUG logging to see individual transformer timing.
        """
        logger.info("Starting transform phase with %d transformers", len(self.transforms))
        start_time = time.time()

        for i, transform in enumerate(self.transforms):
            transform_start_time = time.time()
            logger.debug("Running transformer %d/%d: %s", i, len(self.transforms), transform.id_)
            transform.transform()
            transform_time = time.time() - transform_start_time
            logger.debug("Transformer %s completed in %.2f seconds", transform.id_, transform_time)

        phase_time = time.time() - start_time
        logger.info("Transform phase completed successfully in %.2f seconds", phase_time)

    @trace_span("spark_job._load")
    def _load(self) -> None:
        """Write transformed data to all configured destinations.

        Executes each load component sequentially, writing data from the
        DataFrameRegistry to target destinations (CSV, JSON, databases, etc.).
        Load components retrieve their input data from the registry based on
        configuration references.

        Raises:
            Exception: If any load operation fails. The phase stops at the
                first failure without executing subsequent loaders.

        Note:
            Load is always the final phase. All transforms must complete
            successfully before data is written. Enable DEBUG logging to see
            individual loader timing.
        """
        logger.info("Starting load phase with %d loaders", len(self.loads))
        start_time = time.time()

        for i, load in enumerate(self.loads):
            load_start_time = time.time()
            logger.debug("Running loader %d/%d: %s", i, len(self.loads), load.id_)
            load.load()
            load_time = time.time() - load_start_time
            logger.debug("Loader %s completed in %.2f seconds", load.id_, load_time)

        phase_time = time.time() - start_time
        logger.info("Load phase completed successfully in %.2f seconds", phase_time)

    @override
    def _clear(self) -> None:
        """Free resources by clearing Spark-specific registries.

        Clears the DataFrameRegistry and StreamingQueryRegistry after job execution
        completes. This prevents memory leaks and ensures clean state for subsequent
        jobs, particularly important in long-running processes or batch environments
        where multiple jobs execute sequentially.
        """
        logger.debug("Clearing DataFrameRegistry after job: %s", self.id_)
        DataFrameRegistry().clear()

        logger.debug("Clearing StreamingQueryRegistry after job: %s", self.id_)
        StreamingQueryRegistry().clear()
