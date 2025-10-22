"""Apache Spark job execution engine for data pipeline orchestration.

This package provides the PySpark-based implementation for executing
configuration-driven data pipelines. It enables users to define extract,
transform, and load operations through configuration files, which are then
executed using Apache Spark as the processing backend.
"""

from samara.runtime.jobs.spark import extract, job

# Make commonly used components available at the top level
__all__ = [
    "extract",
    "job",
]
