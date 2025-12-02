"""Job models for the ETL framework configuration system.

This module defines the core job model and execution engine abstraction.
It enables users to configure complete ETL pipelines through job definitions
that specify data extraction, transformation, and loading operations.
"""

import logging
from abc import ABC, abstractmethod
from enum import Enum
from typing import Generic, Self, TypeVar

from pydantic import Field, model_validator

from samara import BaseModel
from samara.exceptions import SamaraWorkflowError
from samara.telemetry import (
    ATTR_JOB_ENABLED,
    ATTR_JOB_ENGINE,
    ATTR_JOB_EXTRACT_COUNT,
    ATTR_JOB_ID,
    ATTR_JOB_LOAD_COUNT,
    ATTR_JOB_TRANSFORM_COUNT,
    add_span_event,
    set_span_attributes,
    set_span_error,
    set_span_ok,
    trace_span,
)
from samara.utils.logger import get_logger
from samara.workflow.jobs.hooks import Hooks
from samara.workflow.jobs.models.model_extract import ExtractModel
from samara.workflow.jobs.models.model_load import LoadModel
from samara.workflow.jobs.models.model_transform import TransformModel

logger: logging.Logger = get_logger(__name__)

ExtractT = TypeVar("ExtractT", bound=ExtractModel)
TransformT = TypeVar("TransformT", bound=TransformModel)
LoadT = TypeVar("LoadT", bound=LoadModel)


class JobEngine(Enum):
    """Define available execution engines for ETL job processing.

    Specifies the supported execution engines that process configured
    data pipelines. Used as the discriminator to route jobs to their
    appropriate implementation handlers.

    Example:
        **Configuration in JSON:**
        ```
        {
            "id": "daily_sync",
            "engine": "spark",
            "extracts": [],
            "transforms": [],
            "loads": []
        }
        ```

        **Configuration in YAML:**
        ```
        id: daily_sync
        engine: spark
        extracts: []
        transforms: []
        loads: []
        ```

    Note:
        Future engines (Polars, Pandas) can be added to support
        different execution characteristics and performance profiles.
    """

    SPARK = "spark"
    # Future engines can be added here:
    # POLARS = "polars"


class JobModel(BaseModel, ABC, Generic[ExtractT, TransformT, LoadT]):
    """Execute a complete ETL pipeline with configuration-driven stages.

    Orchestrates data pipeline execution through configurable extract,
    transform, and load stages. Handles job lifecycle hooks, exception
    management, and registry cleanup. The framework validates all
    component IDs for uniqueness and upstream references to ensure
    configuration correctness before execution.

    This abstract class provides the execution framework that all
    engine-specific implementations (Spark, Polars, Pandas) inherit.
    Subclasses implement _execute() with engine-specific logic and _clear()
    for resource cleanup.

    Attributes:
        id_: Unique identifier for this job within the pipeline configuration.
        description: Human-readable purpose and context for the job.
        enabled: Controls whether this job executes; disabled jobs are skipped.
        engine_type: Specifies which execution engine processes this job.
        extracts: List of data source configurations for reading data.
        transforms: Ordered list of transformation operations applied in sequence.
        loads: List of destination configurations for writing results.
        hooks: Lifecycle hooks executed at onStart, onError, onSuccess, onFinally stages.

    Example:
        **Configuration in JSON:**
        ```
        {
            "id": "customer_orders_daily",
            "description": "Join customer and order data for daily reporting",
            "enabled": true,
            "engine": "spark",
            "extracts": [
                {
                    "id": "customers",
                    "type": "csv",
                    "path": "s3://data/customers"
                },
                {
                    "id": "orders",
                    "type": "json",
                    "path": "s3://data/orders"
                }
            ],
            "transforms": [
                {
                    "id": "filtered_orders",
                    "upstreamId": "orders",
                    "functions": [
                        {
                            "type": "filter",
                            "expression": "amount > 0"
                        }
                    ]
                },
                {
                    "id": "enriched_data",
                    "upstreamId": "filtered_orders",
                    "functions": [
                        {
                            "type": "join",
                            "mode": "inner",
                            "otherUpstreamId": "customers",
                            "keys": ["customer_id"]
                        }
                    ]
                }
            ],
            "loads": [
                {
                    "id": "final_output",
                    "upstreamId": "enriched_data",
                    "type": "csv",
                    "path": "s3://results/customer_orders"
                }
            ],
            "hooks": {
                "onSuccess": []
            }
        }
        ```

        **Configuration in YAML:**
        ```
        id: customer_orders_daily
        description: Join customer and order data for daily reporting
        enabled: true
        engine: spark
        extracts:
          - id: customers
            type: csv
            path: s3://data/customers
          - id: orders
            type: json
            path: s3://data/orders
        transforms:
          - id: filtered_orders
            upstreamId: orders
            functions:
              - type: filter
                expression: amount > 0
          - id: enriched_data
            upstreamId: filtered_orders
            functions:
              - type: join
                mode: inner
                otherUpstreamId: customers
                keys:
                  - customer_id
        loads:
          - id: final_output
            upstreamId: enriched_data
            type: csv
            path: s3://results/customer_orders
        hooks:
          onSuccess: []
        ```

    Note:
        Job execution automatically clears all DataFrames and streaming queries
        from registries after completion (success or failure) to prevent memory
        leaks and cross-job data contamination. Transform order matters: later
        transforms can only reference earlier extracts or transforms in the list.
    """

    id_: str = Field(..., alias="id", description="Unique identifier for the job", min_length=1)
    description: str = Field(..., description="Human-readable description of the job's purpose")
    enabled: bool = Field(..., description="Whether this job should be executed")
    engine_type: JobEngine = Field(..., description="The execution engine to use for this job")
    extracts: list[ExtractT] = Field(..., description="Collection of Extract components")
    transforms: list[TransformT] = Field(..., description="Collection of Transform components")
    loads: list[LoadT] = Field(..., description="Collection of Load components")
    hooks: Hooks = Field(..., description="Hooks to execute at various stages of the job lifecycle")

    @model_validator(mode="after")
    def validate_unique_ids(self) -> Self:
        """Enforce unique IDs across all job components.

        Validates that all extract, transform, and load IDs are unique
        within this job configuration. Prevents accidental ID collisions
        that would cause upstream reference errors.

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If any ID appears more than once in extracts,
                transforms, or loads sections.
        """
        # Collect all IDs as lists (not sets) to detect duplicates
        extract_ids_list = [extract.id_ for extract in self.extracts]
        transform_ids_list = [transform.id_ for transform in self.transforms]
        load_ids_list = [load.id_ for load in self.loads]

        # Validate unique IDs within the job
        all_ids = extract_ids_list + transform_ids_list + load_ids_list
        duplicates = {id_ for id_ in all_ids if all_ids.count(id_) > 1}
        if duplicates:
            raise ValueError(f"Duplicate IDs found in job '{self.id_}': {', '.join(sorted(duplicates))}")

        return self

    @model_validator(mode="after")
    def validate_upstream_references(self) -> Self:
        """Validate all upstream component references and ordering constraints.

        Checks that:
        - All transform upstream_ids reference existing extracts or previously
          defined transforms (ordered dependency validation)
        - Transforms never self-reference via upstream_id
        - All load upstream_ids reference existing extracts or any transform
        - Join functions reference valid upstream sources

        This validation enables the framework to execute transforms in sequence,
        ensuring each transform's dependencies are satisfied before execution.

        Returns:
            Self: The validated instance.

        Raises:
            ValueError: If invalid upstream_id references, self-references,
                or ordering violations are detected with descriptive messages
                indicating the problematic component and suggestion for correction.
        """
        # Convert to sets for upstream reference validation
        extract_ids = {extract.id_ for extract in self.extracts}

        # Validate transform upstream_ids reference existing extracts or previously defined transforms
        # Build valid upstream IDs progressively as we process transforms in order
        valid_upstream_ids_for_transforms = extract_ids.copy()
        for transform in self.transforms:
            # Check if transform references itself
            if transform.upstream_id == transform.id_:
                raise ValueError(
                    f"Transform '{transform.id_}' references itself as upstream_id "
                    f"in job '{self.id_}'. A transform cannot reference its own id."
                )

            # Check if upstream_id exists in extracts or previously defined transforms
            if transform.upstream_id not in valid_upstream_ids_for_transforms:
                raise ValueError(
                    f"Transform '{transform.id_}' references upstream_id '{transform.upstream_id}' "
                    f"in job '{self.id_}' which either does not exist or is defined later in the transforms list. "
                    f"upstream_id must reference an existing extract or a transform that appears before this one."
                )

            for function in transform.functions:
                if function.function_type == "join":
                    other_upstream_id = function.arguments.other_upstream_id
                    if other_upstream_id not in valid_upstream_ids_for_transforms:
                        raise ValueError(
                            f"Transform '{transform.id_}' with 'join' function references "
                            f"other_upstream_id '{other_upstream_id}' in job '{self.id_}' which "
                            f"either does not exist or is defined later in the transforms list. "
                            f"other_upstream_id must reference an existing extract or a transform "
                            f"that appears before this one."
                        )

            # Add current transform ID to valid upstream IDs for subsequent transforms
            valid_upstream_ids_for_transforms.add(transform.id_)

        # Validate load upstream_ids reference existing extracts or transforms
        transform_ids = {transform.id_ for transform in self.transforms}
        valid_upstream_ids_for_loads = extract_ids | transform_ids
        for load in self.loads:
            if load.upstream_id not in valid_upstream_ids_for_loads:
                raise ValueError(
                    f"Load '{load.id_}' references non-existent upstream_id '{load.upstream_id}' "
                    f"in job '{self.id_}'. upstream_id must reference an existing extract or transform."
                )

        return self

    @trace_span("job.execute")
    def execute(self) -> None:
        """Execute the complete ETL pipeline with lifecycle management.

        Orchestrates job execution including enabled/disabled checks, hook
        execution at key lifecycle points, exception handling, and resource
        cleanup. Wraps configuration and I/O errors in SamaraJobError to
        provide consistent error handling across all engine implementations.

        Lifecycle flow:
        1. Check if job is enabled; skip if disabled
        2. Execute onStart hook
        3. Run _execute() with engine-specific logic
        4. On success: execute onSuccess hook
        5. On error: execute onError hook and wrap in SamaraJobError
        6. Finally: execute onFinally hook and clear all registries

        After execution completes (success or failure), automatically clears
        all DataFrames and streaming queries from engine registries to free
        memory and prevent data leakage between jobs.

        Raises:
            SamaraWorkflowError: Wraps ValueError, KeyError, or OSError exceptions
                with context about the job, preserving the original exception
                as the cause for debugging.
        """
        # Set job-level span attributes for observability
        set_span_attributes({
            ATTR_JOB_ID: self.id_,
            ATTR_JOB_ENGINE: self.engine_type.value,
            ATTR_JOB_ENABLED: self.enabled,
            ATTR_JOB_EXTRACT_COUNT: len(self.extracts),
            ATTR_JOB_TRANSFORM_COUNT: len(self.transforms),
            ATTR_JOB_LOAD_COUNT: len(self.loads),
        })

        if not self.enabled:
            logger.info("Job '%s' is disabled. Skipping execution.", self.id_)
            add_span_event("job.skipped", {"reason": "job_disabled", ATTR_JOB_ID: self.id_})
            set_span_ok()
            return

        add_span_event("job.hooks.on_start", {ATTR_JOB_ID: self.id_})
        self.hooks.on_start()

        try:
            logger.info("Starting job execution: %s", self.id_)
            add_span_event("job.execution.started", {
                ATTR_JOB_ID: self.id_,
                ATTR_JOB_ENGINE: self.engine_type.value,
                ATTR_JOB_EXTRACT_COUNT: len(self.extracts),
                ATTR_JOB_TRANSFORM_COUNT: len(self.transforms),
                ATTR_JOB_LOAD_COUNT: len(self.loads),
            })
            self._execute()
            logger.info("Job completed successfully: %s", self.id_)
            add_span_event("job.execution.completed", {ATTR_JOB_ID: self.id_})
            add_span_event("job.hooks.on_success", {ATTR_JOB_ID: self.id_})
            self.hooks.on_success()
            set_span_ok()
        except (ValueError, KeyError, OSError) as e:
            logger.error("Job '%s' failed: %s", self.id_, e)
            add_span_event("job.execution.failed", {
                ATTR_JOB_ID: self.id_,
                "error.type": type(e).__name__,
                "error.message": str(e),
            })
            set_span_error(e, f"Job '{self.id_}' execution failed")
            add_span_event("job.hooks.on_error", {ATTR_JOB_ID: self.id_})
            self.hooks.on_error()
            raise SamaraWorkflowError(f"Error occurred during job '{self.id_}' execution") from e
        finally:
            add_span_event("job.hooks.on_finally", {ATTR_JOB_ID: self.id_})
            self.hooks.on_finally()
            add_span_event("job.cleanup.started", {ATTR_JOB_ID: self.id_})
            self._clear()
            add_span_event("job.cleanup.completed", {ATTR_JOB_ID: self.id_})

    @abstractmethod
    def _execute(self) -> None:
        """Run engine-specific ETL pipeline logic.

        Implemented by each engine-specific job subclass to execute the
        configured extract, transform, and load operations using the
        appropriate execution engine (Spark, Polars, Pandas, etc.).

        This is called automatically during job execution after validation.
        """

    @abstractmethod
    def _clear(self) -> None:
        """Clear engine registries and free execution resources.

        Implemented by each engine-specific job subclass to clean up all
        data registries (DataFrames, streaming queries, temporary objects)
        after job execution completes. Prevents memory leaks and ensures
        data doesn't leak between independent job executions.

        This is always called during the finally block of execute(),
        regardless of success or failure.
        """
