"""Job type definitions for workflow pipeline execution.

This module provides the discriminated union of all available job types,
allowing the workflow to support multiple processing engines. The types are
defined separately from base models to prevent circular import issues and
maintain clean module boundaries.

Note:
    Currently supports Spark-based jobs. As additional engines (Polars,
    Dask, etc.) are implemented, this module will expand the union type
    to include all available job types with proper discriminator support.
"""

from samara.workflow.jobs.spark.job import JobSpark

# For now, just use JobSpark directly since it's the only engine
# When more engines are added, this will become a discriminated union:
# JobUnion = Annotated[JobSpark | JobPolars, Discriminator("engine")]
JobUnion = JobSpark

__all__ = ["JobUnion", "JobSpark"]
