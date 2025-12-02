"""Spark job - Execute ETL pipelines using Apache Spark.

This module provides the JobSpark class, which orchestrates Extract, Transform,
and Load operations using Spark as the processing engine. It executes configuration-
driven pipelines in sequence, managing data flow from sources through transformations
to destinations with automatic component instantiation.
"""

import logging
import time

from typing_extensions import override

from samara.telemetry import (
    ATTR_COMPONENT_ID,
    ATTR_DURATION_MS,
    ATTR_JOB_ENGINE,
    ATTR_JOB_EXTRACT_COUNT,
    ATTR_JOB_ID,
    ATTR_JOB_LOAD_COUNT,
    ATTR_JOB_TRANSFORM_COUNT,
    ATTR_PHASE_COMPONENT_INDEX,
    ATTR_PHASE_COMPONENT_TOTAL,
    ATTR_PHASE_NAME,
    add_span_event,
    set_span_attributes,
    set_span_ok,
    trace_span,
)
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

        # Set span attributes for job-level observability
        set_span_attributes({
            ATTR_JOB_ID: self.id_,
            ATTR_JOB_ENGINE: self.engine_type.value,
            ATTR_JOB_EXTRACT_COUNT: len(self.extracts),
            ATTR_JOB_TRANSFORM_COUNT: len(self.transforms),
            ATTR_JOB_LOAD_COUNT: len(self.loads),
        })

        logger.info(
            "Starting Spark job execution with %d extracts, %d transforms, %d loads",
            len(self.extracts),
            len(self.transforms),
            len(self.loads),
        )
        add_span_event("spark_job.pipeline.started", {
            ATTR_JOB_ID: self.id_,
            ATTR_JOB_EXTRACT_COUNT: len(self.extracts),
            ATTR_JOB_TRANSFORM_COUNT: len(self.transforms),
            ATTR_JOB_LOAD_COUNT: len(self.loads),
        })

        self._extract()
        self._transform()
        self._load()

        execution_time_ms = (time.time() - start_time) * 1000
        set_span_attributes({ATTR_DURATION_MS: execution_time_ms})
        add_span_event("spark_job.pipeline.completed", {
            ATTR_JOB_ID: self.id_,
            ATTR_DURATION_MS: execution_time_ms,
        })
        logger.info("Spark job completed in %.2f seconds", execution_time_ms / 1000)
        set_span_ok()

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
        set_span_attributes({
            ATTR_PHASE_NAME: "extract",
            ATTR_PHASE_COMPONENT_TOTAL: len(self.extracts),
        })

        logger.info("Starting extract phase with %d extractors", len(self.extracts))
        add_span_event("phase.extract.started", {
            ATTR_PHASE_NAME: "extract",
            ATTR_PHASE_COMPONENT_TOTAL: len(self.extracts),
        })
        start_time = time.time()

        for i, extract in enumerate(self.extracts):
            component_index = i + 1
            extract_start_time = time.time()
            logger.debug("Running extractor %d/%d: %s", component_index, len(self.extracts), extract.id_)
            add_span_event("component.extract.started", {
                ATTR_COMPONENT_ID: extract.id_,
                ATTR_PHASE_COMPONENT_INDEX: component_index,
                ATTR_PHASE_COMPONENT_TOTAL: len(self.extracts),
            })

            extract.extract()

            extract_time_ms = (time.time() - extract_start_time) * 1000
            add_span_event("component.extract.completed", {
                ATTR_COMPONENT_ID: extract.id_,
                ATTR_PHASE_COMPONENT_INDEX: component_index,
                ATTR_DURATION_MS: extract_time_ms,
            })
            logger.debug("Extractor %s completed in %.2f seconds", extract.id_, extract_time_ms / 1000)

        phase_time_ms = (time.time() - start_time) * 1000
        add_span_event("phase.extract.completed", {
            ATTR_PHASE_NAME: "extract",
            ATTR_DURATION_MS: phase_time_ms,
            "samara.phase.components_processed": len(self.extracts),
        })
        logger.info("Extract phase completed successfully in %.2f seconds", phase_time_ms / 1000)
        set_span_ok()

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
        set_span_attributes({
            ATTR_PHASE_NAME: "transform",
            ATTR_PHASE_COMPONENT_TOTAL: len(self.transforms),
        })

        logger.info("Starting transform phase with %d transformers", len(self.transforms))
        add_span_event("phase.transform.started", {
            ATTR_PHASE_NAME: "transform",
            ATTR_PHASE_COMPONENT_TOTAL: len(self.transforms),
        })
        start_time = time.time()

        for i, transform in enumerate(self.transforms):
            component_index = i + 1
            transform_start_time = time.time()
            logger.debug("Running transformer %d/%d: %s", component_index, len(self.transforms), transform.id_)
            add_span_event("component.transform.started", {
                ATTR_COMPONENT_ID: transform.id_,
                ATTR_PHASE_COMPONENT_INDEX: component_index,
                ATTR_PHASE_COMPONENT_TOTAL: len(self.transforms),
            })

            transform.transform()

            transform_time_ms = (time.time() - transform_start_time) * 1000
            add_span_event("component.transform.completed", {
                ATTR_COMPONENT_ID: transform.id_,
                ATTR_PHASE_COMPONENT_INDEX: component_index,
                ATTR_DURATION_MS: transform_time_ms,
            })
            logger.debug("Transformer %s completed in %.2f seconds", transform.id_, transform_time_ms / 1000)

        phase_time_ms = (time.time() - start_time) * 1000
        add_span_event("phase.transform.completed", {
            ATTR_PHASE_NAME: "transform",
            ATTR_DURATION_MS: phase_time_ms,
            "samara.phase.components_processed": len(self.transforms),
        })
        logger.info("Transform phase completed successfully in %.2f seconds", phase_time_ms / 1000)
        set_span_ok()

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
        set_span_attributes({
            ATTR_PHASE_NAME: "load",
            ATTR_PHASE_COMPONENT_TOTAL: len(self.loads),
        })

        logger.info("Starting load phase with %d loaders", len(self.loads))
        add_span_event("phase.load.started", {
            ATTR_PHASE_NAME: "load",
            ATTR_PHASE_COMPONENT_TOTAL: len(self.loads),
        })
        start_time = time.time()

        for i, load in enumerate(self.loads):
            component_index = i + 1
            load_start_time = time.time()
            logger.debug("Running loader %d/%d: %s", component_index, len(self.loads), load.id_)
            add_span_event("component.load.started", {
                ATTR_COMPONENT_ID: load.id_,
                ATTR_PHASE_COMPONENT_INDEX: component_index,
                ATTR_PHASE_COMPONENT_TOTAL: len(self.loads),
            })

            load.load()

            load_time_ms = (time.time() - load_start_time) * 1000
            add_span_event("component.load.completed", {
                ATTR_COMPONENT_ID: load.id_,
                ATTR_PHASE_COMPONENT_INDEX: component_index,
                ATTR_DURATION_MS: load_time_ms,
            })
            logger.debug("Loader %s completed in %.2f seconds", load.id_, load_time_ms / 1000)

        phase_time_ms = (time.time() - start_time) * 1000
        add_span_event("phase.load.completed", {
            ATTR_PHASE_NAME: "load",
            ATTR_DURATION_MS: phase_time_ms,
            "samara.phase.components_processed": len(self.loads),
        })
        logger.info("Load phase completed successfully in %.2f seconds", phase_time_ms / 1000)
        set_span_ok()

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
