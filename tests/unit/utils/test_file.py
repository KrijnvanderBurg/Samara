"""Unit tests for the file handling utility module.

This module contains tests for the file utilities that are responsible
for reading and parsing configuration files in various formats like JSON and YAML.

The tests verify that:
- Files in different formats can be correctly read and parsed
- Error conditions like missing files are properly handled
- The factory pattern correctly selects the appropriate handler for each file type
- File validation methods work correctly for various edge cases
"""

import tempfile
from pathlib import Path
from unittest.mock import patch

import pyjson5 as json
import pytest

from samara.utils.file import FileHandlerContext, FileJsonHandler, FileYamlHandler


class TestFileValidation:
    """Unit tests for file validation through public interface."""

    @pytest.fixture
    def temp_empty_file(self) -> Path:
        """Create a temporary empty file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.flush()
            return Path(temp_file.name)

    @pytest.fixture
    def temp_directory(self) -> Path:
        """Create a temporary directory."""
        temp_dir = tempfile.mkdtemp()
        return Path(temp_dir)

    def test_read_directory_raises_error(self, temp_directory: Path) -> None:
        """Test reading a directory raises OSError."""
        handler = FileJsonHandler(filepath=temp_directory)
        with pytest.raises(OSError):
            handler.read()

    def test_read_empty_file_raises_error(self, temp_empty_file: Path) -> None:
        """Test reading empty file raises OSError."""
        handler = FileJsonHandler(filepath=temp_empty_file)
        with pytest.raises(OSError):
            handler.read()

    def test_read_permission_denied(self) -> None:
        """Test reading file with no read permissions raises PermissionError."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.write(json.dumps({"key": "value"}))
            temp_file.flush()
            temp_path = Path(temp_file.name)

        handler = FileJsonHandler(filepath=temp_path)
        with patch("os.access", return_value=False):
            with pytest.raises(PermissionError):
                handler.read()

    def test_binary_file_with_null_bytes(self) -> None:
        """Test that files with null bytes are detected as binary."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as temp_file:
            temp_file.write('{"key": "value"}\x00')  # Valid JSON with null byte
            temp_file.flush()
            temp_path = Path(temp_file.name)

        handler = FileJsonHandler(filepath=temp_path)
        with pytest.raises(OSError):
            handler.read()

    def test_encoding_error_file(self) -> None:
        """Test that files with encoding errors are caught."""
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".json", delete=False) as temp_file:
            temp_file.write(b"\xff\xfe\x00\x00")  # Invalid UTF-8
            temp_file.flush()
            temp_path = Path(temp_file.name)

        handler = FileJsonHandler(filepath=temp_path)
        with pytest.raises(OSError):
            handler.read()

    def test_file_too_large(self) -> None:
        """Test that files larger than 10MB are rejected."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            # Write more than 10MB of data (10MB = 10 * 1024 * 1024 bytes)
            large_data = "x" * (11 * 1024 * 1024)  # 11MB of data
            temp_file.write(large_data)
            temp_file.flush()
            temp_path = Path(temp_file.name)

        handler = FileJsonHandler(filepath=temp_path)
        with pytest.raises(OSError):
            handler.read()


class TestYamlHandler:
    """Unit tests for FileYamlHandler."""

    @pytest.fixture
    def temp_yaml_file(self) -> Path:
        """Create a temporary YAML file with valid content."""
        yaml_content = "key: value"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            temp_file.write(yaml_content)
            temp_file.flush()
            return Path(temp_file.name)

    def test_read_success(self, temp_yaml_file: Path) -> None:
        """Test reading YAML data from a file."""
        handler = FileYamlHandler(filepath=temp_yaml_file)
        data = handler.read()
        assert data == {"key": "value"}

    def test_read_file_not_found(self) -> None:
        """Test reading non-existent YAML file raises FileNotFoundError."""
        handler = FileYamlHandler(filepath=Path("non_existent_file.yaml"))
        with pytest.raises(FileNotFoundError):
            handler.read()

    def test_read_permission_error_in_open(self, temp_yaml_file: Path) -> None:
        """Test reading YAML file with permission error during open."""
        with patch("builtins.open", side_effect=PermissionError):
            handler = FileYamlHandler(filepath=temp_yaml_file)
            with pytest.raises(PermissionError):
                handler.read()

    def test_read_invalid_yaml(self) -> None:
        """Test reading invalid YAML content raises yaml.YAMLError."""
        invalid_yaml = "key: value:"  # Invalid YAML syntax
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as temp_file:
            temp_file.write(invalid_yaml)
            temp_file.flush()
            temp_path = Path(temp_file.name)

        handler = FileYamlHandler(filepath=temp_path)
        with pytest.raises(ValueError):
            handler.read()


class TestJsonHandler:
    """Unit tests for FileJsonHandler."""

    @pytest.fixture
    def temp_json_file(self) -> Path:
        """Create a temporary JSON file with valid content."""
        json_content = {"key": "value"}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.write(json.dumps(json_content))
            temp_file.flush()
            return Path(temp_file.name)

    def test_read_success(self, temp_json_file: Path) -> None:
        """Test reading JSON data from a file."""
        handler = FileJsonHandler(filepath=temp_json_file)
        data = handler.read()
        assert data == {"key": "value"}

    def test_read_file_not_found(self) -> None:
        """Test reading non-existent JSON file raises FileNotFoundError."""
        handler = FileJsonHandler(filepath=Path("non_existent.json"))
        with pytest.raises(FileNotFoundError):
            handler.read()

    def test_read_permission_error_in_open(self, temp_json_file: Path) -> None:
        """Test reading JSON file with permission error during open."""
        with patch("builtins.open", side_effect=PermissionError):
            handler = FileJsonHandler(filepath=temp_json_file)
            with pytest.raises(PermissionError):
                handler.read()

    def test_read_json_decode_error(self) -> None:
        """Test reading invalid JSON content raises json.JSONDecodeError."""
        invalid_json = '{"name": "John" "age": 30}'  # Missing comma
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as temp_file:
            temp_file.write(invalid_json)
            temp_file.flush()
            temp_path = Path(temp_file.name)

        handler = FileJsonHandler(filepath=temp_path)
        with pytest.raises(ValueError):
            handler.read()


class TestFileHandlerContext:
    """Unit tests for FileHandlerContext factory."""

    @pytest.mark.parametrize(
        "filepath, expected_handler_class",
        [
            (Path("test.yml"), FileYamlHandler),
            (Path("test.yaml"), FileYamlHandler),
            (Path("test.json"), FileJsonHandler),
        ],
    )
    def test_from_filepath_success(self, filepath: Path, expected_handler_class: type) -> None:
        """Test from_filepath returns correct handler for supported extensions."""
        handler = FileHandlerContext.from_filepath(filepath=filepath)
        assert isinstance(handler, expected_handler_class)

    def test_from_filepath_unsupported_extension(self) -> None:
        """Test from_filepath raises NotImplementedError for unsupported extension."""
        with pytest.raises(ValueError):
            FileHandlerContext.from_filepath(Path("test.txt"))
