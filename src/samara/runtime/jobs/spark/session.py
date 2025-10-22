"""PySpark session management for Samara pipelines.

This module manages the Spark execution engine that processes data transformations.
It ensures only one active Spark context exists to prevent resource conflicts and
improve performance.

Key features:
- Lazy initialization of Spark sessions (only created when needed)
- Automatic resource cleanup and management
- Centralized configuration handling for Spark parameters
- Seamless integration with Samara's configuration-driven pipeline model

The SparkHandler singleton ensures efficient resource usage across the entire
pipeline lifecycle, whether running locally for testing or on distributed clusters.
"""

import logging
from typing import Any

from pyspark.sql import SparkSession

from samara.types import Singleton
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SparkHandler(metaclass=Singleton):
    """Manages a single Spark execution engine for data processing.

    SparkHandler ensures that your Samara pipeline uses exactly one Spark session,
    preventing resource conflicts and improving performance. It automatically handles
    Spark initialization and cleanup.

    Key responsibilities:
    - Creates and maintains a single shared Spark session
    - Applies configuration from your pipeline definition
    - Handles resource cleanup when the pipeline completes
    - Provides consistent access to the Spark engine across all pipeline stages

    Under the hood, this uses the Singleton pattern to guarantee only one instance
    exists, regardless of how many parts of your code reference it.

    Attributes:
        _session: The active Spark session (created on first use, not at startup)
        _app_name: Your application's name, used for job tracking in Spark
        _init_options: Configuration settings from your pipeline definition
    """

    _session: SparkSession | None
    _app_name: str
    _init_options: dict[str, str]

    def __init__(
        self,
        app_name: str = "samara",
        options: dict[str, str] | None = None,
    ) -> None:
        """Initialize the Spark handler with your pipeline configuration.

        Prepares the handler with your application name and any Spark settings
        from your pipeline definition. The actual Spark session won't start until
        the first transformation runs.

        Args:
            app_name: Your application name (default: "samara"). Used to identify
                your pipeline in Spark logs and job tracking systems.
            options: Spark configuration from your pipeline definition as key-value
                pairs (e.g., {"spark.executor.memory": "4g"}). Optional.
        """
        logger.debug("Configuring SparkHandler with app_name: %s (lazy initialization)", app_name)
        self._session = None
        self._app_name = app_name
        self._init_options = options or {}

    @property
    def session(self) -> SparkSession:
        """Access the Spark engine for running your pipeline.

        Returns the active Spark session, creating it on first access. This ensures
        Spark only initializes when your pipeline actually needs to process data,
        not during import or configuration parsing.

        Returns:
            The Spark session ready to execute your transformations
        """
        if self._session is None:
            logger.debug("Creating SparkSession on first access - app_name: %s", self._app_name)

            builder = SparkSession.Builder().appName(name=self._app_name)

            if self._init_options:
                for key, value in self._init_options.items():
                    logger.debug("Setting Spark config: %s = %s", key, value)
                    builder = builder.config(key=key, value=value)

            logger.debug("Creating/retrieving SparkSession")
            self._session = builder.getOrCreate()
            logger.info("SparkHandler initialized successfully with app: %s", self._app_name)

        logger.debug("Accessing SparkSession instance")
        return self._session

    @session.setter
    def session(self, session: SparkSession) -> None:
        """Replace the current Spark session.

        Sets a new Spark session. This is typically used internally by the
        framework and rarely needed in pipeline definitions.

        Args:
            session: The Spark session to use for subsequent operations
        """
        logger.debug(
            "Setting SparkSession instance - app name: %s, version: %s", session.sparkContext.appName, session.version
        )
        self._session = session

    @session.deleter
    def session(self) -> None:
        """Stop and clean up the Spark session.

        Properly shuts down the active Spark session and releases all associated
        resources. This is called automatically when your pipeline completes or
        encounters a fatal error.

        Use this to manually clean up if you need to restart Spark during a
        pipeline's lifecycle.
        """
        if self._session is not None:
            logger.info("Stopping SparkSession: %s", self._session.sparkContext.appName)
            self._session.stop()
            self._session = None
            logger.info("SparkSession stopped and cleaned up successfully")
        else:
            logger.debug("SparkSession was never initialized, nothing to stop")

    def add_configs(self, options: dict[str, Any]) -> None:
        """Apply additional Spark settings at runtime.

        Adds or updates Spark configuration options after the engine has started.
        This is useful when certain settings depend on runtime conditions discovered
        during pipeline execution.

        Args:
            options: Configuration settings as key-value pairs to apply
                (e.g., {"spark.sql.shuffle.partitions": "200"})

        Note:
            Some Spark settings cannot be changed after initialization. For
            pre-execution configuration, define settings in your pipeline's
            engine configuration instead.
        """
        logger.debug("Adding %d configuration options to SparkSession", len(options))

        for key, value in options.items():
            logger.debug("Setting runtime config: %s = %s", key, value)
            self.session.conf.set(key=key, value=value)

        logger.info("Successfully applied %d configuration options", len(options))
