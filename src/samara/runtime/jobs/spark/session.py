"""PySpark session management utilities.

This module provides a singleton implementation of SparkSession management to ensure
that only one active Spark context exists within the application. It includes:

- SparkHandler class for creating and accessing a shared SparkSession
- Utility functions for configuring Spark with sensible defaults
- Helper methods for common Spark operations

The singleton pattern ensures resource efficiency and prevents issues that can
arise from multiple concurrent Spark contexts.
"""

import logging
from typing import Any

from pyspark.sql import SparkSession

from samara.types import Singleton
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


class SparkHandler(metaclass=Singleton):
    """Singleton handler for SparkSession management.

    Ensures that only one SparkSession is active throughout the application
    lifecycle, preventing resource conflicts and improving performance.

    This class uses the Singleton metaclass to ensure that only one instance
    is created regardless of how many times it's initialized.

    The SparkSession is created lazily on first access to avoid unnecessary
    initialization when Spark is not actually used.

    Attributes:
        _session: The managed PySpark SparkSession instance (created lazily)
        _app_name: Application name for the SparkSession
        _init_options: Initial configuration options for the SparkSession
    """

    _session: SparkSession | None
    _app_name: str
    _init_options: dict[str, str]

    def __init__(
        self,
        app_name: str = "samara",
        options: dict[str, str] | None = None,
    ) -> None:
        """Initialize the SparkHandler with app name and configuration options.

        Stores configuration for lazy initialization. The SparkSession is not
        created until the session property is first accessed.

        Args:
            app_name: Name of the Spark application, used for tracking and monitoring
            options: Optional dictionary of Spark configuration options as key-value pairs
        """
        logger.debug("Configuring SparkHandler with app_name: %s (lazy initialization)", app_name)
        self._session = None
        self._app_name = app_name
        self._init_options = options or {}

    @property
    def session(self) -> SparkSession:
        """Get the current managed SparkSession instance.

        Lazily creates the SparkSession on first access. This ensures that
        Spark is only initialized when actually needed, not when the module
        is imported.

        Returns:
            The current active SparkSession instance
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
        """Set the managed SparkSession instance.

        Updates the internal reference to the SparkSession instance.
        This is typically only used internally during initialization.

        Args:
            session: The SparkSession instance to use
        """
        logger.debug(
            "Setting SparkSession instance - app name: %s, version: %s", session.sparkContext.appName, session.version
        )
        self._session = session

    @session.deleter
    def session(self) -> None:
        """Stop and delete the current SparkSession.

        Properly terminates the SparkSession and removes the internal reference.
        This ensures that all Spark resources are released cleanly.

        This should be called when the SparkSession is no longer needed,
        typically at the end of the application lifecycle.
        """
        if self._session is not None:
            logger.info("Stopping SparkSession: %s", self._session.sparkContext.appName)
            self._session.stop()
            self._session = None
            logger.info("SparkSession stopped and cleaned up successfully")
        else:
            logger.debug("SparkSession was never initialized, nothing to stop")

    def add_configs(self, options: dict[str, Any]) -> None:
        """Add configuration options to the active SparkSession.

        Updates the configuration of the current SparkSession with new options.
        This can be used to modify Spark behavior at runtime, although not all
        configuration options can be changed after the session is created.

        Args:
            options: Dictionary of configuration key-value pairs to apply

        Note:
            Some Spark configurations can only be set during initialization
            and cannot be changed using this method after the SparkSession
            has been created.
        """
        logger.debug("Adding %d configuration options to SparkSession", len(options))

        for key, value in options.items():
            logger.debug("Setting runtime config: %s = %s", key, value)
            self.session.conf.set(key=key, value=value)

        logger.info("Successfully applied %d configuration options", len(options))
