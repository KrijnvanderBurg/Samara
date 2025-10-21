# // {
# //     "id": "archive-processed-files",
# //     "description": "",
# //     "enabled": true,
# //     "action_type": "move_or_copy_job_files",
# //     "operation": "move",                        // allowed: "move" | "copy"
# //     "source_location": "incoming/daily/",       // filepath, directory, or "dataframe"
# //     "destination_location": "archive/daily/",   // filepath or directory (base target)
# //     "hierarchy_base_path": "incoming/daily/",   // optional; root used when preserving source hierarchy
# //     "duplicate_handling": {
# //         "dedupe_by": ["size", "checksum"],      // options: "checksum" | "size"
# //         "on_match": "skip",
# // destination already exists and is identical -> allowed: "overwrite" | "version"
# //         "on_mismatch": "version",
# // destination already exists but is not identical -> allowed: "overwrite" | "version" | "fail" | "notify"
# //         "checksum": {
# //             "algorithm": "sha256",              // allowed: "md5", "sha1", "sha256"
# //             "chunk_size_bytes": 65536,          // default 65536 (64 KiB)
# //             "verify_checksum_after_transfer": false    // optional post-transfer verification
# //         },
# //         "version": {
# //             "datetime_format": "yyyyMMddHHmmss", // format applied when creating versioned filenames
# //             "timestamp_timezone": "UTC"
# //         }
# //     }
# // }


# """Module for moving files as an action in Samara runtime hooks."""

# import hashlib
# import logging
# import shutil
# from datetime import datetime
# from pathlib import Path
# from typing import Literal
# from zoneinfo import ZoneInfo

# from pydantic import field_validator

# from samara import BaseModel
# from samara.runtime.actions.base import ActionBase

# logger = logging.getLogger(__name__)


# class Version(BaseModel):
#     """Model to handle versioning of files.

#     Attributes:
#         datetime_format: The strftime format string for timestamp (e.g., '%Y%m%d_%H%M%S').
#         timestamp_timezone: The timezone for the timestamp (e.g., 'UTC', 'America/New_York').
#     """

#     datetime_format: str
#     timestamp_timezone: str


# class Checksum(BaseModel):
#     """Model to handle file checksum verification.

#     Attributes:
#         algorithm: The checksum algorithm to use (e.g., 'sha256', 'sha512', 'blake2b').
#         chunk_size_bytes: The size of chunks to read when calculating checksums.
#         verify_checksum_after_transfer: Whether to verify checksum after file transfer.
#     """

#     algorithm: str
#     chunk_size_bytes: int
#     verify_checksum_after_transfer: bool

#     @field_validator("algorithm")
#     @classmethod
#     def validate_algorithm(cls, v: str) -> str:
#         """Validate and normalize the algorithm name.

#         Args:
#             v: The algorithm name.

#         Returns:
#             The lowercase algorithm name.

#         Raises:
#             ValueError: If the algorithm is not supported by hashlib.
#         """
#         algorithm_lower = v.lower()
#         try:
#             # Test if the algorithm is available
#             hashlib.new(algorithm_lower)
#         except ValueError as exc:
#             raise ValueError(f"Unsupported checksum algorithm: {v}. Algorithm must be supported by hashlib.") from exc
#         return algorithm_lower


# class DuplicateHandling(BaseModel):
#     """Model to handle duplicate file scenarios.

#     Attributes:
#         dedupe_by: List of strategies to check for duplicates in order (e.g., ['size', 'checksum']).
#         on_match: Action when a duplicate is found ('skip', 'overwrite', 'rename').
#         on_mismatch: Action when files exist but aren't duplicates ('overwrite', 'version', 'rename', 'skip').
#         checksum: Checksum configuration for verification.
#         version: Versioning configuration for file naming.
#     """

#     dedupe_by: list[Literal["size", "checksum", "name"]]
#     on_match: Literal["skip", "overwrite", "rename"]
#     on_mismatch: Literal["overwrite", "version", "rename", "skip"]
#     checksum: Checksum
#     version: Version


# class MoveOrCopyJobFiles(ActionBase):
#     """Action to move or copy files from source to destination with duplicate handling.

#     This action processes files from a source location and moves or copies them to a destination,
#     handling duplicates based on configurable strategies. It supports file deduplication by size
#     and checksum, with options for overwrite, skip, rename, or versioning.

#     Args:
#         action: The action identifier, must be 'move_or_copy_job_files'.
#         source_location: The source file or directory path.
#         destination_location: The destination directory path.
#         hierarchy_base_path: The base path used to construct destination hierarchy.
#         operation: The file operation to perform ('move' or 'copy').
#         duplicate_handling: Configuration for handling duplicate files.
#     """

#     action: Literal["move_or_copy_job_files"]
#     source_location: str
#     destination_location: str
#     hierarchy_base_path: str
#     operation: Literal["move", "copy"]
#     duplicate_handling: DuplicateHandling

#     def _execute(self) -> None:
#         """Execute the move or copy files action."""
#         source_path = Path(self.source_location)
#         destination_base = Path(self.destination_location)
#         hierarchy_base = Path(self.hierarchy_base_path)

#         # Validate source exists
#         if not source_path.exists():
#             error_msg = f"Source path does not exist: {source_path}"
#             logger.error(error_msg)
#             raise FileNotFoundError(error_msg)

#         # Ensure destination base exists
#         destination_base.mkdir(parents=True, exist_ok=True)

#         # Process files
#         if source_path.is_file():
#             self._process_file(source_path, destination_base, hierarchy_base)
#         elif source_path.is_dir():
#             self._process_directory(source_path, destination_base, hierarchy_base)
#         else:
#             error_msg = f"Source path is neither a file nor a directory: {source_path}"
#             logger.error(error_msg)
#             raise ValueError(error_msg)

#     def _process_directory(self, source_dir: Path, destination_base: Path, hierarchy_base: Path) -> None:
#         """Process all files in a directory recursively.

#         Args:
#             source_dir: The source directory to process.
#             destination_base: The base destination directory.
#             hierarchy_base: The base path for constructing destination hierarchy.
#         """
#         for item in source_dir.rglob("*"):
#             if item.is_file():
#                 self._process_file(item, destination_base, hierarchy_base)

#     def _process_file(self, source_file: Path, destination_base: Path, hierarchy_base: Path) -> None:
#         """Process a single file: determine destination and handle duplicates.

#         Args:
#             source_file: The source file to process.
#             destination_base: The base destination directory.
#             hierarchy_base: The base path for constructing destination hierarchy.
#         """
#         # Construct destination path maintaining hierarchy
#         destination_path = self._construct_destination_path(source_file, destination_base, hierarchy_base)

#         # Ensure destination directory exists
#         destination_path.parent.mkdir(parents=True, exist_ok=True)

#         # Check if destination file exists
#         if destination_path.exists():
#             # Handle duplicate file
#             self._handle_duplicate(source_file, destination_path)
#         else:
#             # No duplicate, check on_mismatch strategy
#             self._handle_new_file(source_file, destination_path)

#     def _construct_destination_path(self, source_file: Path, destination_base: Path, hierarchy_base: Path) -> Path:
#         """Construct the destination path by maintaining hierarchy relative to base path.

#         Args:
#             source_file: The source file path.
#             destination_base: The base destination directory.
#             hierarchy_base: The base path for hierarchy calculation.

#         Returns:
#             The constructed destination path.
#         """
#         try:
#             relative_path = source_file.relative_to(hierarchy_base)
#         except ValueError:
#             # If source is not relative to hierarchy_base, use just the filename
#             relative_path = source_file.name

#         return destination_base / relative_path

#     def _handle_duplicate(self, source_file: Path, destination_file: Path) -> None:
#         """Handle a duplicate file based on deduplication strategies.

#         Args:
#             source_file: The source file path.
#             destination_file: The existing destination file path.
#         """
#         is_duplicate = self._is_duplicate(source_file, destination_file)

#         if is_duplicate:
#             # Files are duplicates, use on_match strategy
#             logger.info(f"Duplicate file detected: {source_file} -> {destination_file}")
#             self._apply_on_match_strategy(source_file, destination_file)
#         else:
#             # Files are not duplicates, use on_mismatch strategy
#             logger.info(f"File exists but not a duplicate: {source_file} -> {destination_file}")
#             self._apply_on_mismatch_strategy(source_file, destination_file)

#     def _is_duplicate(self, source_file: Path, destination_file: Path) -> bool:
#         """Determine if two files are duplicates based on configured strategies.

#         Checks each strategy in order. If any strategy determines files are different,
#         they are not duplicates. All strategies must confirm similarity for a duplicate.

#         Args:
#             source_file: The source file path.
#             destination_file: The destination file path.

#         Returns:
#             True if files are duplicates based on all checked strategies, False otherwise.
#         """
#         for strategy in self.duplicate_handling.dedupe_by:
#             if strategy == "size":
#                 if not self._compare_size(source_file, destination_file):
#                     logger.debug(f"Files differ by size: {source_file}")
#                     return False
#             elif strategy == "checksum":
#                 if not self._compare_checksum(source_file, destination_file):
#                     logger.debug(f"Files differ by checksum: {source_file}")
#                     return False
#             elif strategy == "name":
#                 if not self._compare_name(source_file, destination_file):
#                     logger.debug(f"Files differ by name: {source_file}")
#                     return False
#             else:
#                 logger.warning(f"Unknown deduplication strategy: {strategy}")

#         # All strategies confirmed similarity
#         return True

#     def _compare_size(self, file1: Path, file2: Path) -> bool:
#         """Compare file sizes.

#         Args:
#             file1: First file path.
#             file2: Second file path.

#         Returns:
#             True if files have the same size, False otherwise.
#         """
#         return file1.stat().st_size == file2.stat().st_size

#     def _compare_checksum(self, file1: Path, file2: Path) -> bool:
#         """Compare file checksums using configured algorithm.

#         Args:
#             file1: First file path.
#             file2: Second file path.

#         Returns:
#             True if files have the same checksum, False otherwise.
#         """
#         checksum1 = self._calculate_checksum(file1)
#         checksum2 = self._calculate_checksum(file2)
#         return checksum1 == checksum2

#     def _compare_name(self, file1: Path, file2: Path) -> bool:
#         """Compare file names.

#         Args:
#             file1: First file path.
#             file2: Second file path.

#         Returns:
#             True if files have the same name, False otherwise.
#         """
#         return file1.name == file2.name

#     def _calculate_checksum(self, file_path: Path) -> str:
#         """Calculate file checksum using configured algorithm and chunk size.

#         Args:
#             file_path: The file to calculate checksum for.

#         Returns:
#             The hexadecimal checksum string.

#         Raises:
#             ValueError: If the checksum algorithm is not supported.
#         """
#         # Algorithm is already validated and normalized to lowercase by Pydantic
#         algorithm = self.duplicate_handling.checksum.algorithm
#         chunk_size = self.duplicate_handling.checksum.chunk_size_bytes

#         try:
#             hash_obj = hashlib.new(algorithm)
#         except ValueError as exc:
#             error_msg = f"Unsupported checksum algorithm: {algorithm}"
#             logger.error(error_msg)
#             raise ValueError(error_msg) from exc

#         with file_path.open("rb") as file:
#             while chunk := file.read(chunk_size):
#                 hash_obj.update(chunk)

#         return hash_obj.hexdigest()

#     def _apply_on_match_strategy(self, source_file: Path, destination_file: Path) -> None:
#         """Apply the configured strategy when a duplicate is matched.

#         Args:
#             source_file: The source file path.
#             destination_file: The destination file path.

#         Raises:
#             ValueError: If an invalid strategy is configured (should not happen with Pydantic validation).
#         """
#         strategy = self.duplicate_handling.on_match

#         if strategy == "skip":
#             logger.info(f"Skipping duplicate file: {source_file}")
#             # Do nothing, file already exists
#         elif strategy == "overwrite":
#             logger.info(f"Overwriting duplicate file: {destination_file}")
#             self._transfer_file(source_file, destination_file)
#         elif strategy == "rename":
#             logger.info(f"Renaming and transferring file: {source_file}")
#             new_destination = self._generate_renamed_path(destination_file)
#             self._transfer_file(source_file, new_destination)
#         else:
#             # This should never happen due to Pydantic validation
#             error_msg = f"Invalid on_match strategy: {strategy}"
#             logger.error(error_msg)
#             raise ValueError(error_msg)

#     def _apply_on_mismatch_strategy(self, source_file: Path, destination_file: Path) -> None:
#         """Apply the configured strategy when files exist but are not duplicates.

#         Args:
#             source_file: The source file path.
#             destination_file: The destination file path.

#         Raises:
#             ValueError: If an invalid strategy is configured (should not happen with Pydantic validation).
#         """
#         strategy = self.duplicate_handling.on_mismatch

#         if strategy == "overwrite":
#             logger.info(f"Overwriting non-duplicate file: {destination_file}")
#             self._transfer_file(source_file, destination_file)
#         elif strategy == "version":
#             logger.info(f"Versioning file: {destination_file}")
#             versioned_destination = self._generate_versioned_path(destination_file)
#             self._transfer_file(source_file, versioned_destination)
#         elif strategy == "rename":
#             logger.info(f"Renaming file: {destination_file}")
#             new_destination = self._generate_renamed_path(destination_file)
#             self._transfer_file(source_file, new_destination)
#         elif strategy == "skip":
#             logger.info(f"Skipping non-duplicate file: {source_file}")
#         else:
#             # This should never happen due to Pydantic validation
#             error_msg = f"Invalid on_mismatch strategy: {strategy}"
#             logger.error(error_msg)
#             raise ValueError(error_msg)

#     def _handle_new_file(self, source_file: Path, destination_file: Path) -> None:
#         """Handle a new file that doesn't exist at destination.

#         Args:
#             source_file: The source file path.
#             destination_file: The destination file path.
#         """
#         logger.info(f"Transferring new file: {source_file} -> {destination_file}")
#         self._transfer_file(source_file, destination_file)

#     def _transfer_file(self, source_file: Path, destination_file: Path) -> None:
#         """Transfer (move or copy) a file from source to destination.

#         Args:
#             source_file: The source file path.
#             destination_file: The destination file path.

#         Raises:
#             IOError: If file transfer fails or checksum verification fails.
#             ValueError: If an invalid operation is configured (should not happen with Pydantic validation).
#         """
#         try:
#             if self.operation == "move":
#                 shutil.move(str(source_file), str(destination_file))
#                 logger.debug(f"Moved file: {source_file} -> {destination_file}")
#             elif self.operation == "copy":
#                 shutil.copy2(str(source_file), str(destination_file))
#                 logger.debug(f"Copied file: {source_file} -> {destination_file}")
#             else:
#                 # This should never happen due to Pydantic validation
#                 error_msg = f"Invalid operation: {self.operation}"
#                 logger.error(error_msg)
#                 raise ValueError(error_msg)

#             # Verify checksum after transfer if configured
#             if self.duplicate_handling.checksum.verify_checksum_after_transfer:
#                 if self.operation == "copy":
#                     # For copy, verify source and destination match
#                     if not self._compare_checksum(source_file, destination_file):
#                         error_msg = f"Checksum verification failed after copy: {destination_file}"
#                         logger.error(error_msg)
#                         raise IOError(error_msg)
#                     logger.debug(f"Checksum verified after copy: {destination_file}")
#                 # For move, source no longer exists, so we can't verify

#         except (OSError, IOError) as exc:
#             error_msg = f"Failed to transfer file {source_file} -> {destination_file}: {exc}"
#             logger.error(error_msg)
#             raise IOError(error_msg) from exc

#     def _generate_versioned_path(self, file_path: Path) -> Path:
#         """Generate a versioned file path with timestamp.

#         Args:
#             file_path: The original file path.

#         Returns:
#             A new path with timestamp appended before the extension.
#         """
#         datetime_format = self.duplicate_handling.version.datetime_format
#         timezone_str = self.duplicate_handling.version.timestamp_timezone

#         try:
#             timezone = ZoneInfo(timezone_str)
#         except Exception as exc:
#             logger.warning(f"Invalid timezone '{timezone_str}', using UTC: {exc}")
#             timezone = ZoneInfo("UTC")

#         timestamp = datetime.now(tz=timezone).strftime(datetime_format)
#         stem = file_path.stem
#         suffix = file_path.suffix

#         versioned_name = f"{stem}_{timestamp}{suffix}"
#         return file_path.parent / versioned_name

#     def _generate_renamed_path(self, file_path: Path) -> Path:
#         """Generate a renamed file path by appending a counter.

#         Args:
#             file_path: The original file path.

#         Returns:
#             A new path with a counter appended to ensure uniqueness.
#         """
#         stem = file_path.stem
#         suffix = file_path.suffix
#         parent = file_path.parent
#         counter = 1

#         while True:
#             new_name = f"{stem}_{counter}{suffix}"
#             new_path = parent / new_name
#             if not new_path.exists():
#                 return new_path
#             counter += 1
