"""
Core type definitions and patterns for the framework.

Provides foundational building blocks for the framework's architecture, including
singleton services, dynamic component registries, and type-safe collections. These
patterns enable configuration-driven pipelines by supporting component registration,
shared resource management, and runtime selection based on configuration definitions
rather than hardcoded implementations.
"""

import threading
from collections.abc import Iterator
from typing import Any, Generic, TypeVar

from pyspark.sql import DataFrame
from pyspark.sql.streaming.query import StreamingQuery

# Type variables with more specific constraints
K = TypeVar("K")  # Key type
V = TypeVar("V")  # Value type


class Singleton(type):
    """Metaclass for creating singleton classes with thread-safe instance management.

    Ensures only one instance of a class is created across the entire application lifecycle.
    This is essential for Samara's registries and shared services that need to maintain
    state across the ETL pipeline execution. Thread-safe through internal locking.

    Attributes:
        _instances (dict[type, Any]): Maps classes to their singleton instances.
        _lock (threading.Lock): Ensures thread-safe instance creation.

    Example:
        >>> class Logger(metaclass=Singleton):
        ...     def __init__(self):
        ...         self.logs = []
        >>> logger1 = Logger()
        >>> logger2 = Logger()
        >>> logger1 is logger2
        True
    """

    _instances: dict[Any, Any] = {}
    _lock: threading.Lock = threading.Lock()

    def __call__(cls, *args: Any, **kwargs: Any) -> Any:
        """Create or retrieve the singleton instance with thread-safe initialization.

        Args:
            *args: Positional arguments passed to the class constructor.
            **kwargs: Keyword arguments passed to the class constructor.

        Returns:
            The singleton instance of the class (same across all invocations).
        """
        with cls._lock:
            if cls not in cls._instances:
                # Assigning super().__call__ to a variable is crucial,
                # as the value of cls is changed in __call__
                instance = super(Singleton, cls).__call__(*args, **kwargs)
                cls._instances[cls] = instance
        return cls._instances[cls]


class RegistryInstance(Generic[K, V], metaclass=Singleton):
    """Generic singleton registry providing dictionary-like access to typed items.

    Combines the Singleton pattern with a type-safe registry to maintain a single,
    shared collection of items throughout the ETL pipeline execution. Used as the
    base for specialized registries like `DataFrameRegistry` and `StreamingQueryRegistry`
    that manage pipeline state and intermediate results.

    Type Parameters:
        K: Key type for item lookup and retrieval.
        V: Value type stored in the registry.

    Example:
        >>> IntRegistry = RegistryInstance[str, int]
        >>> registry = IntRegistry()
        >>> registry['count'] = 42
        >>> registry['count']
        42
        >>> 'count' in registry
        True
    """

    def __init__(self) -> None:
        """Initialize an empty registry instance."""
        self._items: dict[K, V] = {}

    # Instance registry methods
    def __setitem__(self, name: K, item: V) -> None:
        """Store or update an item with the given key.

        Args:
            name: Registry key.
            item: Value to store (replaces any existing item).
        """
        self._items[name] = item

    def __getitem__(self, name: K) -> V:
        """Retrieve an item by key.

        Args:
            name: Registry key.

        Returns:
            The stored value.

        Raises:
            KeyError: If the key is not found.
        """
        try:
            return self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __delitem__(self, name: K) -> None:
        """Remove an item by key.

        Args:
            name: Registry key.

        Raises:
            KeyError: If the key is not found.
        """
        try:
            del self._items[name]
        except KeyError as e:
            raise KeyError(f"Item '{name}' not found.") from e

    def __contains__(self, name: K) -> bool:
        """Check if a key exists in the registry.

        Args:
            name: Registry key.

        Returns:
            True if the key is in the registry, False otherwise.
        """
        return name in self._items

    def __len__(self) -> int:
        """Get the number of items in the registry.

        Returns:
            Count of stored items.
        """
        return len(self._items)

    def __iter__(self) -> Iterator[V]:
        """Iterate over stored items (not keys).

        Returns:
            Iterator over all values in the registry.
        """
        return iter(self._items.values())

    def clear(self) -> None:
        """Clear all items from the registry and release references.

        Removes all stored items to reset the registry state. This enables
        garbage collection of referenced objects.
        """
        self._items.clear()


class DataFrameRegistry(RegistryInstance[str, DataFrame]):
    """Singleton registry for managing intermediate DataFrames throughout the ETL pipeline.

    Maintains named DataFrame objects that represent pipeline stages and intermediate
    results. Integrates with Spark's memory management by explicitly unpersisting
    cached DataFrames during cleanup to prevent memory leaks in long-running pipelines.

    Example:
        >>> registry = DataFrameRegistry()
        >>> registry["customers"] = customers_df
        >>> registry["orders"] = orders_df
        >>> result = registry["customers"].filter(...)
        >>> registry.clear()  # Releases Spark memory
    """

    def __getitem__(self, name: str) -> DataFrame:
        """Retrieve a DataFrame by name with enhanced error context.

        Args:
            name: Name of the DataFrame to retrieve.

        Returns:
            The requested DataFrame.

        Raises:
            KeyError: If the DataFrame is not found. Includes list of available
                      DataFrames in the error message for debugging.
        """
        try:
            return super().__getitem__(name)
        except KeyError as e:
            available = list(self._items.keys())
            raise KeyError(f"DataFrame '{name}' not found. Available DataFrames: {available}") from e

    def clear(self) -> None:
        """Clear all DataFrames and release Spark memory resources.

        Unpersists all cached/persisted DataFrames to free up memory before clearing,
        ensuring Spark releases all associated resources. Important for long-running
        pipelines to prevent memory exhaustion.
        """
        for df in self._items.values():
            if df.storageLevel.useMemory or df.storageLevel.useDisk:
                df.unpersist()
        super().clear()


class StreamingQueryRegistry(RegistryInstance[str, StreamingQuery]):
    """Singleton registry for managing active streaming queries in the ETL pipeline.

    Maintains named StreamingQuery objects for continuous data processing jobs.
    Integrates with Spark Streaming's lifecycle management by properly stopping
    active queries during cleanup to prevent resource leaks and orphaned processes.

    Example:
        >>> registry = StreamingQueryRegistry()
        >>> registry["events"] = events_df.writeStream.start()
        >>> query = registry["events"]
        >>> query.isActive
        True
        >>> registry.clear()  # Stops streams gracefully
    """

    def __getitem__(self, name: str) -> StreamingQuery:
        """Retrieve a StreamingQuery by name with enhanced error context.

        Args:
            name: Name of the StreamingQuery to retrieve.

        Returns:
            The requested StreamingQuery.

        Raises:
            KeyError: If the StreamingQuery is not found. Includes list of active
                      queries in the error message for debugging.
        """
        try:
            return super().__getitem__(name)
        except KeyError as e:
            available = list(self._items.keys())
            raise KeyError(f"StreamingQuery '{name}' not found. Available queries: {available}") from e

    def clear(self) -> None:
        """Clear all streaming queries and stop active streams.

        Stops all active streaming queries to gracefully release Spark Streaming
        resources. Important to prevent orphaned processes and resource exhaustion
        when restarting or shutting down the pipeline.
        """
        for query in self._items.values():
            if query.isActive:
                query.stop()
        super().clear()
