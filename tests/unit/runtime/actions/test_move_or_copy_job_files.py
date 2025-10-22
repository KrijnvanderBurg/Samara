# """Unit tests for the MoveOrCopyJobFiles action class."""

# from pathlib import Path
# from tempfile import TemporaryDirectory

# import pytest

# from samara.runtime.actions.move_or_copy_job_files import (
#     Checksum,
#     DuplicateHandling,
#     MoveOrCopyJobFiles,
#     Version,
# )


# class TestMoveOrCopyJobFiles:
#     """Test cases for MoveOrCopyJobFiles action."""

#     def test_execute__source_does_not_exist__raises_file_not_found_error(self) -> None:
#         """Test that execute raises FileNotFoundError when source doesn't exist."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             action = MoveOrCopyJobFiles(
#                 name="test_move",
#                 description="Test move action",
#                 action="move_or_copy_job_files",
#                 source_location=f"{temp_dir}/nonexistent",
#                 destination_location=f"{temp_dir}/dest",
#                 hierarchy_base_path=temp_dir,
#             )

#             # Assert
#             with pytest.raises(FileNotFoundError):
#                 # Act
#                 action.execute()

#     def test_execute__single_file_no_duplicate__transfers_successfully(self) -> None:
#         """Test that execute transfers a single file when no duplicate exists."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             source_file.write_text("test content")

#             dest_dir = Path(temp_dir) / "dest"

#             action = MoveOrCopyJobFiles(
#                 name="test_copy",
#                 description="Test copy action",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_dir),
#                 hierarchy_base_path=temp_dir,
#                 operation="copy",
#             )

#             # Act
#             action.execute()

#             # Assert
#             dest_file = dest_dir / "source.txt"
#             assert dest_file.exists()
#             assert dest_file.read_text() == "test content"
#             assert source_file.exists()  # Original still exists for copy

#     def test_execute__single_file_with_move_operation__removes_source(self) -> None:
#         """Test that execute removes source file when operation is move."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             source_file.write_text("test content")

#             dest_dir = Path(temp_dir) / "dest"

#             action = MoveOrCopyJobFiles(
#                 name="test_move",
#                 description="Test move action",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_dir),
#                 hierarchy_base_path=temp_dir,
#                 operation="move",
#             )

#             # Act
#             action.execute()

#             # Assert
#             dest_file = dest_dir / "source.txt"
#             assert dest_file.exists()
#             assert dest_file.read_text() == "test content"
#             assert not source_file.exists()  # Source removed for move

#     def test_execute__directory_with_multiple_files__transfers_all_files(self) -> None:
#         """Test that execute transfers all files in a directory."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_dir = Path(temp_dir) / "source"
#             source_dir.mkdir()

#             (source_dir / "file1.txt").write_text("content1")
#             (source_dir / "file2.txt").write_text("content2")

#             subdir = source_dir / "subdir"
#             subdir.mkdir()
#             (subdir / "file3.txt").write_text("content3")

#             dest_dir = Path(temp_dir) / "dest"

#             action = MoveOrCopyJobFiles(
#                 name="test_dir_copy",
#                 description="Test directory copy action",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_dir),
#                 destination_location=str(dest_dir),
#                 hierarchy_base_path=str(source_dir),
#                 operation="copy",
#             )

#             # Act
#             action.execute()

#             # Assert
#             assert (dest_dir / "file1.txt").exists()
#             assert (dest_dir / "file2.txt").exists()
#             assert (dest_dir / "subdir" / "file3.txt").exists()
#             assert (dest_dir / "file1.txt").read_text() == "content1"
#             assert (dest_dir / "subdir" / "file3.txt").read_text() == "content3"

#     def test_is_duplicate__same_size_and_checksum__returns_true(self) -> None:
#         """Test that is_duplicate returns True when files have same size and checksum."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             file1 = Path(temp_dir) / "file1.txt"
#             file2 = Path(temp_dir) / "file2.txt"
#             content = "identical content"

#             file1.write_text(content)
#             file2.write_text(content)

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(file1),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#             )

#             # Act
#             result = action._is_duplicate(file1, file2)

#             # Assert
#             assert result is True

#     def test_is_duplicate__different_size__returns_false(self) -> None:
#         """Test that is_duplicate returns False when files have different sizes."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             file1 = Path(temp_dir) / "file1.txt"
#             file2 = Path(temp_dir) / "file2.txt"

#             file1.write_text("short")
#             file2.write_text("much longer content")

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(file1),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#             )

#             # Act
#             result = action._is_duplicate(file1, file2)

#             # Assert
#             assert result is False

#     def test_is_duplicate__same_size_different_checksum__returns_false(self) -> None:
#         """Test that is_duplicate returns False when files have same size but different checksums."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             file1 = Path(temp_dir) / "file1.txt"
#             file2 = Path(temp_dir) / "file2.txt"

#             # Same length but different content
#             file1.write_text("12345")
#             file2.write_text("abcde")

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(file1),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#             )

#             # Act
#             result = action._is_duplicate(file1, file2)

#             # Assert
#             assert result is False

#     def test_handle_duplicate__on_match_skip__does_not_transfer_file(self) -> None:
#         """Test that duplicate with on_match=skip doesn't transfer the file."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             dest_file = Path(temp_dir) / "dest.txt"

#             source_file.write_text("content")
#             dest_file.write_text("content")

#             original_mtime = dest_file.stat().st_mtime

#             duplicate_handling = DuplicateHandling(on_match="skip")

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_file.parent),
#                 hierarchy_base_path=temp_dir,
#                 duplicate_handling=duplicate_handling,
#             )

#             # Act
#             action._handle_duplicate(source_file, dest_file)

#             # Assert
#             assert dest_file.stat().st_mtime == original_mtime  # File not modified

#     def test_handle_duplicate__on_match_overwrite__overwrites_file(self) -> None:
#         """Test that duplicate with on_match=overwrite overwrites the file."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             dest_file = Path(temp_dir) / "dest.txt"

#             source_file.write_text("new content")
#             dest_file.write_text("new content")

#             duplicate_handling = DuplicateHandling(on_match="overwrite")

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_file.parent),
#                 hierarchy_base_path=temp_dir,
#                 operation="copy",
#                 duplicate_handling=duplicate_handling,
#             )

#             # Act
#             action._handle_duplicate(source_file, dest_file)

#             # Assert
#             assert dest_file.read_text() == "new content"

#     def test_handle_duplicate__on_match_rename__creates_renamed_file(self) -> None:
#         """Test that duplicate with on_match=rename creates a renamed file."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             dest_file = Path(temp_dir) / "dest.txt"

#             source_file.write_text("content")
#             dest_file.write_text("content")

#             duplicate_handling = DuplicateHandling(on_match="rename")

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_file.parent),
#                 hierarchy_base_path=temp_dir,
#                 operation="copy",
#                 duplicate_handling=duplicate_handling,
#             )

#             # Act
#             action._handle_duplicate(source_file, dest_file)

#             # Assert
#             renamed_file = Path(temp_dir) / "dest_1.txt"
#             assert renamed_file.exists()
#             assert renamed_file.read_text() == "content"

#     def test_handle_duplicate__on_mismatch_version__creates_versioned_file(self) -> None:
#         """Test that non-duplicate with on_mismatch=version creates a versioned file."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             dest_file = Path(temp_dir) / "dest.txt"

#             source_file.write_text("new content")
#             dest_file.write_text("old content")

#             version = Version(datetime_format="%Y%m%d_%H%M%S", timestamp_timezone="UTC")
#             duplicate_handling = DuplicateHandling(on_mismatch="version", version=version)

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_file.parent),
#                 hierarchy_base_path=temp_dir,
#                 operation="copy",
#                 duplicate_handling=duplicate_handling,
#             )

#             # Act
#             action._handle_duplicate(source_file, dest_file)

#             # Assert
#             # Find the versioned file
#             versioned_files = list(Path(temp_dir).glob("dest_*.txt"))
#             assert len(versioned_files) == 1
#             assert versioned_files[0].read_text() == "new content"

#     def test_calculate_checksum__valid_algorithm__returns_correct_checksum(self) -> None:
#         """Test that calculate_checksum returns correct checksum for valid algorithm."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             test_file = Path(temp_dir) / "test.txt"
#             test_file.write_text("test content")

#             checksum_config = Checksum(algorithm="sha256", chunk_size_bytes=1024)
#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(test_file),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#                 duplicate_handling=DuplicateHandling(checksum=checksum_config),
#             )

#             # Act
#             checksum = action._calculate_checksum(test_file)

#             # Assert
#             assert isinstance(checksum, str)
#             assert len(checksum) == 64  # SHA256 produces 64 hex characters

#     def test_calculate_checksum__invalid_algorithm__raises_value_error(self) -> None:
#         """Test that calculate_checksum raises ValueError for invalid algorithm."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             test_file = Path(temp_dir) / "test.txt"
#             test_file.write_text("test content")

#             checksum_config = Checksum(algorithm="invalid_algo", chunk_size_bytes=1024)
#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(test_file),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#                 duplicate_handling=DuplicateHandling(checksum=checksum_config),
#             )

#             # Assert
#             with pytest.raises(ValueError):
#                 # Act
#                 action._calculate_checksum(test_file)

#     def test_construct_destination_path__maintains_hierarchy(self) -> None:
#         """Test that construct_destination_path maintains directory hierarchy."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             hierarchy_base = Path(temp_dir) / "base"
#             hierarchy_base.mkdir()

#             subdir = hierarchy_base / "sub1" / "sub2"
#             subdir.mkdir(parents=True)

#             source_file = subdir / "file.txt"
#             source_file.write_text("content")

#             dest_base = Path(temp_dir) / "destination"

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_base),
#                 hierarchy_base_path=str(hierarchy_base),
#             )

#             # Act
#             result = action._construct_destination_path(source_file, dest_base, hierarchy_base)

#             # Assert
#             expected = dest_base / "sub1" / "sub2" / "file.txt"
#             assert result == expected

#     def test_construct_destination_path__source_not_relative_to_base__uses_filename_only(self) -> None:
#         """Test that construct_destination_path uses filename only when source not relative to base."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             hierarchy_base = Path(temp_dir) / "base"
#             hierarchy_base.mkdir()

#             source_file = Path(temp_dir) / "other" / "file.txt"
#             source_file.parent.mkdir(parents=True)
#             source_file.write_text("content")

#             dest_base = Path(temp_dir) / "destination"

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_base),
#                 hierarchy_base_path=str(hierarchy_base),
#             )

#             # Act
#             result = action._construct_destination_path(source_file, dest_base, hierarchy_base)

#             # Assert
#             expected = dest_base / "file.txt"
#             assert result == expected

#     def test_generate_versioned_path__creates_timestamped_filename(self) -> None:
#         """Test that generate_versioned_path creates filename with timestamp."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             test_file = Path(temp_dir) / "test.txt"

#             version = Version(datetime_format="%Y%m%d", timestamp_timezone="UTC")
#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(test_file),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#                 duplicate_handling=DuplicateHandling(version=version),
#             )

#             # Act
#             result = action._generate_versioned_path(test_file)

#             # Assert
#             assert result.parent == test_file.parent
#             assert result.suffix == ".txt"
#             assert result.stem.startswith("test_")
#             assert len(result.stem) > 5  # Has timestamp appended

#     def test_generate_renamed_path__creates_unique_filename_with_counter(self) -> None:
#         """Test that generate_renamed_path creates unique filename with counter."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             test_file = Path(temp_dir) / "test.txt"
#             test_file.write_text("content")

#             # Create conflicting files
#             (Path(temp_dir) / "test_1.txt").write_text("existing")
#             (Path(temp_dir) / "test_2.txt").write_text("existing")

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(test_file),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#             )

#             # Act
#             result = action._generate_renamed_path(test_file)

#             # Assert
#             expected = Path(temp_dir) / "test_3.txt"
#             assert result == expected

#     def test_transfer_file__with_checksum_verification__verifies_successfully(self) -> None:
#         """Test that transfer_file verifies checksum after copy when configured."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             source_file = Path(temp_dir) / "source.txt"
#             dest_file = Path(temp_dir) / "dest.txt"

#             source_file.write_text("test content for verification")

#             checksum_config = Checksum(algorithm="sha256", chunk_size_bytes=1024, verify_checksum_after_transfer=True)

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(source_file),
#                 destination_location=str(dest_file.parent),
#                 hierarchy_base_path=temp_dir,
#                 operation="copy",
#                 duplicate_handling=DuplicateHandling(checksum=checksum_config),
#             )

#             # Act
#             action._transfer_file(source_file, dest_file)

#             # Assert
#             assert dest_file.exists()
#             assert dest_file.read_text() == "test content for verification"

#     def test_dedupe_by_size_only__skips_checksum_for_different_sizes(self) -> None:
#         """Test that dedupe_by with only size skips checksum calculation for different sizes."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             file1 = Path(temp_dir) / "file1.txt"
#             file2 = Path(temp_dir) / "file2.txt"

#             file1.write_text("short")
#             file2.write_text("much longer content")

#             duplicate_handling = DuplicateHandling(dedupe_by=["size"])

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(file1),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#                 duplicate_handling=duplicate_handling,
#             )

#             # Act
#             result = action._is_duplicate(file1, file2)

#             # Assert
#             assert result is False

#     def test_dedupe_by_name__compares_filenames(self) -> None:
#         """Test that dedupe_by with name compares filenames."""
#         # Arrange
#         with TemporaryDirectory() as temp_dir:
#             file1 = Path(temp_dir) / "file1.txt"
#             file2 = Path(temp_dir) / "file2.txt"

#             file1.write_text("content")
#             file2.write_text("content")

#             duplicate_handling = DuplicateHandling(dedupe_by=["name"])

#             action = MoveOrCopyJobFiles(
#                 name="test",
#                 description="Test",
#                 action="move_or_copy_job_files",
#                 source_location=str(file1),
#                 destination_location=temp_dir,
#                 hierarchy_base_path=temp_dir,
#                 duplicate_handling=duplicate_handling,
#             )

#             # Act
#             result = action._is_duplicate(file1, file2)

#             # Assert
#             assert result is False  # Different names


# class TestChecksumModel:
#     """Test cases for Checksum model."""

#     def test_checksum_model__with_defaults__has_correct_values(self) -> None:
#         """Test that Checksum model has correct default values."""
#         # Act
#         checksum = Checksum()

#         # Assert
#         assert checksum.algorithm == "sha256"
#         assert checksum.chunk_size_bytes == 65536
#         assert checksum.verify_checksum_after_transfer is True


# class TestVersionModel:
#     """Test cases for Version model."""

#     def test_version_model__with_defaults__has_correct_values(self) -> None:
#         """Test that Version model has correct default values."""
#         # Act
#         version = Version()

#         # Assert
#         assert version.datetime_format == "%Y%m%d_%H%M%S"
#         assert version.timestamp_timezone == "UTC"


# class TestDuplicateHandlingModel:
#     """Test cases for DuplicateHandling model."""

#     def test_duplicate_handling_model__with_defaults__has_correct_values(self) -> None:
#         """Test that DuplicateHandling model has correct default values."""
#         # Act
#         handling = DuplicateHandling()

#         # Assert
#         assert handling.dedupe_by == ["size", "checksum"]
#         assert handling.on_match == "skip"
#         assert handling.on_mismatch == "overwrite"
#         assert isinstance(handling.checksum, Checksum)
#         assert isinstance(handling.version, Version)
