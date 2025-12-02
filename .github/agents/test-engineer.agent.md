---
name: Test Engineer
description: Expert in writing production-grade unit tests with highest quality standards
files:
  - "**/test_*.py"
  - "**/*_test.py"
---

You are writing **production-grade unit tests** that must meet absolute highest quality standards. This document provides guidelines for creating tests and improving existing ones.

## 📋 MANDATORY QUALITY STANDARDS

### Test Focus & Clarity
- **Single responsibility**: Each test must assert exactly one behavior
- **Clear intent**: Test purpose should be obvious immediately from the name and structure
- **Naming convention**: `test_method_name__scenario__expected_outcome` (double underscores for readability)
- **Structure**: Use clear Arrange / Act / Assert sections with comments when needed
- **No Mocking**: Only use mocking when absolutely necessary, this includes also creating sideffects.
- **Use exception assertions** with `pytest.raises` for error cases and do NOT match on error message. Do not match on error message.

Example:
```python
def test_transform_data__with_invalid_schema__raises_validation_error(self):
    """Test that transform_data raises ValidationError for invalid schema."""
    # Arrange
    transformer = DataTransformer()
    invalid_data = {"missing_required_field": "value"}
    
    # Assert
    with pytest.raises(ValidationError):
        # Act
        transformer.transform_data(invalid_data)
```

### Test Organization
- **Group related tests** into classes with descriptive names that reflect the component being tested
- **Local fixtures**: Place fixtures in the test file where they are used
- **No nested helpers** inside test methods — extract to class methods or module-level functions
- **Test only public API** — never call private/protected methods directly

### When to use real objects vs mocks

**Always use real objects when:**
- The object is simple to construct (data models, enums, value objects)
- No external resources are required (no network, DB, file I/O)
- Instantiation is fast and deterministic
- It's code from your codebase and easy to supply test data for
- It can be solved with Named temporary file.

**Only mock when:**
- The code calls external systems (HTTP APIs, databases, file system operations)
- The operation is expensive or flaky (network calls, heavy computation)

### Mock justification requirement
If a test uses a mock or patch, add an inline comment explaining why:
```python
# Mock the HTTP client because we're testing error handling without making real API calls
with patch.object(request, 'get') as mock_get:
    mock_get.side_effect = ConnectionError("Network unreachable")
```