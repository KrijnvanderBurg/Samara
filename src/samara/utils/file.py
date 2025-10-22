"""File handling utilities - Load and validate configuration files.

This module provides factory-based file handling for reading JSON and YAML
configuration files with comprehensive validation. It enables consistent parsing
of pipeline definitions across different file formats through a common interface.
"""

import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import pyjson5 as json
import yaml

from samara.utils.logger import get_logger

logger = get_logger(__name__)


class FileHandler(ABC):
    """Abstract base for file reading and validation operations.

    Defines the interface for loading and validating files, handling different
    file formats (JSON, YAML) with comprehensive validation including existence
    checks, permissions, size limits, and format verification. Subclasses must
    implement the `_read()` method to parse format-specific content.

    Attributes:
        filepath: Path to the file being handled.
        DEFAULT_MAX_SIZE: Maximum allowed file size (10 MB by default).
        ENCODING: Character encoding used for file reading (UTF-8).

    Example:
        >>> handler = FileYamlHandler(Path("pipeline.yaml"))
        >>> config = handler.read()  # Validates and reads file
    """

    # Class constants for validation limits
    DEFAULT_MAX_SIZE: int = 10 * 1024 * 1024  # 10 MB
    ENCODING: str = "utf-8"

    def __init__(self, filepath: Path) -> None:
        """Initialize a file handler with the target file path.

        Args:
            filepath: Path object pointing to the file to handle.

        Note:
            File access is deferred until read operations are performed.
            Validation occurs at read time, not during initialization.
        """
        logger.debug("Initializing %s for path: %s", self.__class__.__name__, filepath)
        self.filepath = filepath
        logger.debug("%s initialized successfully for: %s", self.__class__.__name__, filepath)

    def _file_exists(self) -> None:
        """Verify the file exists.

        Raises:
            FileNotFoundError: If the file does not exist.
            OSError: If a system-level error occurs while checking existence.
        """
        logger.debug("Checking file existence: %s", self.filepath)
        if not self.filepath.exists():
            logger.error("File not found: %s", self.filepath)
            raise FileNotFoundError(f"File not found: {self.filepath}")
        logger.debug("File exists: %s", self.filepath)

    def _is_file(self) -> None:
        """Verify the path points to a regular file (not a directory).

        Raises:
            IsADirectoryError: If the path is a directory.
            OSError: If the path is not a regular file or a system-level error occurs.
        """
        logger.debug("Checking if path is a regular file: %s", self.filepath)
        if not self.filepath.is_file():
            logger.error("Path is not a regular file: %s", self.filepath)
            raise OSError(f"Expected a file but found directory or invalid path: '{self.filepath}'")
        logger.debug("Path is a regular file: %s", self.filepath)

    def _read_permission(self) -> None:
        """Verify the file is readable by the current process.

        Raises:
            PermissionError: If the file is not readable.
            OSError: If a system-level error occurs while checking permissions.
        """
        logger.debug("Checking read permissions for file: %s", self.filepath)
        if not os.access(self.filepath, os.R_OK):
            logger.error("Read permission denied for file: %s", self.filepath)
            raise PermissionError(f"Permission denied: Cannot read file '{self.filepath}'")
        logger.debug("Read permissions validated for file: %s", self.filepath)

    def _file_not_empty(self) -> None:
        """Verify the file contains data (not empty).

        Raises:
            OSError: If the file is empty or a system-level error occurs accessing metadata.
        """
        logger.debug("Checking if file is empty: %s", self.filepath)
        file_size = self.filepath.stat().st_size
        if file_size == 0:
            logger.error("File is empty: %s", self.filepath)
            raise OSError(f"File is empty: {self.filepath}")
        logger.debug("File not empty: %s (size: %d bytes)", self.filepath, file_size)

    def _file_size_limits(self, max_size: int = DEFAULT_MAX_SIZE) -> None:
        """Verify file size is within specified limits.

        Args:
            max_size: Maximum allowed file size in bytes (defaults to 10 MB).

        Raises:
            OSError: If the file exceeds size limits or a system-level error occurs.
        """
        logger.debug("Checking file size limits for: %s (max allowed: %d bytes)", self.filepath, max_size)
        file_size = self.filepath.stat().st_size

        if file_size > max_size:
            logger.error(
                "File exceeds size limit: %s (size: %d bytes, maximum: %d bytes)", self.filepath, file_size, max_size
            )
            raise OSError(f"File too large: '{self.filepath}' ({file_size:,} bytes exceeds {max_size:,} bytes limit)")

        logger.debug("File size within limits: %s (size: %d bytes, max: %d bytes)", self.filepath, file_size, max_size)

    def _text_file(self) -> None:
        """Verify the file contains readable text (not binary data).

        Raises:
            OSError: If the file contains binary content or has encoding issues.
            PermissionError: If permission is denied while reading the file.
        """
        logger.debug("Validating file is readable text: %s", self.filepath)
        try:
            with self.filepath.open("r", encoding=self.ENCODING) as file:
                # Read first 512 bytes to check for binary content
                sample = file.read(512)
                if "\x00" in sample:
                    logger.error("File contains binary content: %s", self.filepath)
                    raise OSError(f"Invalid file format: '{self.filepath}' contains binary data, expected text file")
                logger.debug("Text file validation passed: %s", self.filepath)
        except UnicodeDecodeError as e:
            logger.error("File encoding error (not valid UTF-8): %s - %s", self.filepath, e)
            raise OSError(f"Invalid file encoding: '{self.filepath}' is not valid UTF-8") from e

    def read(self) -> dict[str, Any]:
        """Read and validate the file, returning its contents as a dictionary.

        Performs comprehensive validation including existence, file type, permissions,
        size limits, and format checks before parsing. Format-specific parsing is
        delegated to subclass implementations.

        Returns:
            dict[str, Any]: File contents parsed into a dictionary structure.

        Raises:
            FileNotFoundError: If the file does not exist.
            PermissionError: If the file cannot be read due to permission restrictions.
            OSError: If the file fails validation (empty, too large, binary, wrong type).
            NotImplementedError: If the subclass does not implement `_read()`.
        """
        logger.info("Starting file validation and reading: %s", self.filepath)
        logger.debug("Running validation checks for file: %s", self.filepath)

        self._file_exists()
        self._is_file()
        self._read_permission()
        self._file_not_empty()
        self._file_size_limits()
        self._text_file()

        logger.info("All validation checks passed for file: %s", self.filepath)

        logger.debug("Reading file content: %s", self.filepath)
        data = self._read()
        logger.info("File successfully read and parsed: %s", self.filepath)
        return data

    @abstractmethod
    def _read(self) -> dict[str, Any]:
        """Parse and return file contents as a dictionary.

        Subclasses must implement this method to handle format-specific parsing logic.

        Returns:
            dict[str, Any]: File contents parsed into a dictionary structure.

        Raises:
            ValueError: If the file content cannot be parsed.
            FileNotFoundError: If the file does not exist.
            PermissionError: If permission is denied while accessing the file.
            OSError: If a system-level error occurs while reading the file.
        """


class FileYamlHandler(FileHandler):
    """Handle reading and parsing YAML configuration files.

    Loads YAML files with safe parsing to prevent code execution vulnerabilities.
    Complements `FileJsonHandler` for format flexibility in pipeline configurations.
    """

    def _read(self) -> dict[str, Any]:
        """Parse YAML file content into a dictionary.

        Returns:
            dict[str, Any]: YAML content parsed into a dictionary structure.

        Raises:
            ValueError: If the YAML syntax is invalid.
            FileNotFoundError: If the file does not exist.
            PermissionError: If the file cannot be read due to permission restrictions.
        """
        logger.info("Reading YAML file: %s", self.filepath)

        try:
            logger.debug("Opening YAML file for reading: %s", self.filepath)
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                data = yaml.safe_load(file)
                logger.info("Successfully parsed YAML file: %s", self.filepath)
                logger.debug("YAML data structure type: %s", type(data))
                return data
        except yaml.YAMLError as e:
            logger.error("YAML parsing error in file '%s': %s", self.filepath, e)
            raise ValueError(f"Invalid YAML syntax in file '{self.filepath}': {e}") from e


class FileJsonHandler(FileHandler):
    """Handle reading and parsing JSON and JSONC configuration files.

    Loads JSON and JSON with Comments (JSONC) files using a lenient parser that
    supports common extensions like comments and trailing commas. Provides format
    flexibility for pipeline configuration definitions.
    """

    def _read(self) -> dict[str, Any]:
        """Parse JSON or JSONC file content into a dictionary.

        Returns:
            dict[str, Any]: JSON content parsed into a dictionary structure.

        Raises:
            ValueError: If the JSON syntax is invalid.
            FileNotFoundError: If the file does not exist.
            PermissionError: If the file cannot be read due to permission restrictions.
        """
        logger.info("Reading JSON file: %s", self.filepath)

        try:
            logger.debug("Opening JSON file for reading: %s", self.filepath)
            with open(file=self.filepath, mode="r", encoding="utf-8") as file:
                content = file.read()
                data = json.loads(content)
                logger.info("Successfully parsed JSON file: %s", self.filepath)
                logger.debug("JSON data structure type: %s", type(data))
                return data
        except json.Json5DecoderException as e:
            logger.error("JSON parsing error in file '%s': %s", self.filepath, e)
            raise ValueError(f"Invalid JSON syntax in file '{self.filepath}': {e}") from e


class FileHandlerContext:
    """Factory for creating file handlers based on file extension.

    Provides a registry-based factory that automatically selects the appropriate
    file handler (YAML or JSON) based on file extension, enabling polymorphic
    file handling through a single unified interface.

    Attributes:
        SUPPORTED_EXTENSIONS: Maps file extensions (.yaml, .yml, .json, .jsonc)
            to their corresponding handler classes.

    Example:
        >>> handler = FileHandlerContext.from_filepath(Path("config.yaml"))
        >>> data = handler.read()  # Returns parsed configuration
    """

    SUPPORTED_EXTENSIONS: dict[str, type[FileHandler]] = {
        ".yml": FileYamlHandler,
        ".yaml": FileYamlHandler,
        ".json": FileJsonHandler,
        ".jsonc": FileJsonHandler,
    }

    @classmethod
    def from_filepath(cls, filepath: Path) -> FileHandler:
        """Create the appropriate file handler for the given file path.

        Examines the file extension and instantiates the matching handler class.
        Supports YAML (.yaml, .yml) and JSON/JSONC (.json, .jsonc) formats.

        Args:
            filepath: Path to the file requiring a handler.

        Returns:
            FileHandler: An instance of the appropriate handler for the file format.

        Raises:
            ValueError: If the file extension is not supported. Supported formats
                are listed in the error message along with SUPPORTED_EXTENSIONS.

        Example:
            >>> yaml_handler = FileHandlerContext.from_filepath(Path("pipeline.yaml"))
            >>> json_handler = FileHandlerContext.from_filepath(Path("config.jsonc"))
        """
        logger.debug("Creating file handler for path: %s", filepath)
        _, file_extension = os.path.splitext(filepath)
        logger.debug("Detected file extension: %s", file_extension)

        handler_class = cls.SUPPORTED_EXTENSIONS.get(file_extension)

        if handler_class is None:
            supported_extensions = ", ".join(cls.SUPPORTED_EXTENSIONS.keys())
            logger.error(
                "Unsupported file extension '%s' for file: %s. Supported extensions: %s",
                file_extension,
                filepath,
                supported_extensions,
            )
            raise ValueError(
                f"Unsupported file format '{file_extension}' for file '{filepath}'. "
                f"Supported formats: {supported_extensions}"
            )

        logger.debug("Selected handler class: %s for extension: %s", handler_class.__name__, file_extension)
        handler = handler_class(filepath=filepath)
        logger.info("Created %s for file: %s", handler_class.__name__, filepath)
        return handler
