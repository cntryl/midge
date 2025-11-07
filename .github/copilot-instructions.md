# GitHub Copilot Instructions for Midge Project

## Test Writing Guidelines - STRICTLY ENFORCE

When generating or suggesting tests, **ALWAYS** follow these rules:

### 1. Naming Convention (MANDATORY)

- ✅ Use `should_*` naming pattern
- ❌ NEVER use `test_*` naming
- Format: `should_{action}_{condition}_given_{context}`

```rust
// ✅ CORRECT
#[test]
fn should_return_value_when_key_exists() { }

// ❌ WRONG - Will fail meta-test!
#[test]
fn test_get_value() { }
```

### 2. AAA Structure (MANDATORY for tests >5 lines)

Every test MUST have exactly these three comments:

```rust
#[test]
fn should_do_something() {
    // Arrange
    let setup = create_test_data();

    // Act
    let result = perform_operation(setup);

    // Assert
    assert_eq!(result, expected);
}
```

**NEVER use:**

- ❌ `// Arrange & Act` (combined)
- ❌ `// Act & Assert` (combined)
- ❌ `// Setup` (use Arrange)
- ❌ `// Arrange: setup data` (no suffixes)
- ❌ Descriptive AAA comments like `// Arrange - create database`

**ALWAYS use:**

- ✅ Exactly `// Arrange` (no suffix, no combination)
- ✅ Exactly `// Act` (no suffix, no combination)
- ✅ Exactly `// Assert` (no suffix, no combination)

### 3. Single Behavior Principle (PEDANTIC RULE)

**CRITICAL: If each assert_eq! describes a different input-output mapping, create separate tests.**

```rust
// ❌ WRONG - Testing 3 different inputs
#[test]
fn should_return_files_at_level() {
    let l0 = manifest.files_at_level(0);
    let l1 = manifest.files_at_level(1);
    let l2 = manifest.files_at_level(2);
    assert_eq!(l0.len(), 1);  // Different input!
    assert_eq!(l1.len(), 2);  // Different input!
    assert_eq!(l2.len(), 0);  // Different input!
}

// ✅ CORRECT - 3 focused tests
#[test]
fn should_return_files_at_level_zero() {
    // Arrange
    let manifest = setup_with_level_0_files();

    // Act
    let result = manifest.files_at_level(0);

    // Assert
    assert_eq!(result.len(), 1);
}

#[test]
fn should_return_files_at_level_one() {
    // Arrange
    let manifest = setup_with_level_1_files();

    // Act
    let result = manifest.files_at_level(1);

    // Assert
    assert_eq!(result.len(), 2);
}
```

**Exception:** Multiple assertions checking facets of ONE property are OK:

```rust
// ✅ CORRECT - All assertions verify one operation
#[test]
fn should_preserve_data_across_save_load() {
    // Arrange
    let original = create_manifest();

    // Act
    let loaded = save_and_load(original);

    // Assert
    assert_eq!(loaded.id, original.id);      // ✅ Same operation
    assert_eq!(loaded.name, original.name);  // ✅ Same operation
    assert_eq!(loaded.size, original.size);  // ✅ Same operation
}
```

### 4. No Multiple Act Sections

**NEVER have multiple `// Act` comments in one test.**

```rust
// ❌ WRONG - Two operations
#[test]
fn should_upload_and_download() {
    // Arrange
    let backend = Backend::new();

    // Act
    backend.upload("data");  // First operation

    // Assert
    assert_eq!(backend.count(), 1);

    // Act  // ❌ SECOND ACT - WRONG!
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");
}

// ✅ CORRECT - Split into 2 tests
#[test]
fn should_upload_data_successfully() {
    // Arrange
    let backend = Backend::new();

    // Act
    backend.upload("data");

    // Assert
    assert_eq!(backend.count(), 1);
}

#[test]
fn should_download_uploaded_data() {
    // Arrange
    let backend = Backend::new();
    backend.upload("data");

    // Act
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");
}
```

### 5. Small Tests Can Omit AAA

Tests with ≤5 lines don't need AAA comments, but still need proper naming:

```rust
// ✅ CORRECT - Small test, no AAA needed
#[test]
fn should_create_default_config() {
    let config = Config::default();
    assert_eq!(config.timeout, 30);
}
```

## Common Patterns

### Testing Serialization/Deserialization

**ALWAYS split serialize and deserialize into separate tests:**

```rust
// ✅ CORRECT
#[test]
fn should_serialize_manifest() {
    // Arrange
    let manifest = create_manifest();

    // Act
    let result = serde_json::to_string(&manifest);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_deserialize_manifest() {
    // Arrange
    let original = create_manifest();
    let json = serde_json::to_string(&original).unwrap();

    // Act
    let deserialized: Manifest = serde_json::from_str(&json).unwrap();

    // Assert
    assert_eq!(deserialized.id, original.id);
}
```

### Testing Multiple Scenarios

Create separate tests for each scenario:

```rust
// ✅ CORRECT - Separate tests per scenario
#[test]
fn should_return_value_when_key_exists() {
    // Arrange
    let db = Database::new();
    db.insert("key", "value");

    // Act
    let result = db.get("key");

    // Assert
    assert_eq!(result, Some("value"));
}

#[test]
fn should_return_none_when_key_does_not_exist() {
    // Arrange
    let db = Database::new();

    // Act
    let result = db.get("nonexistent");

    // Assert
    assert_eq!(result, None);
}
```

### Table-Driven Tests (When Appropriate)

Use for same operation with different inputs:

```rust
#[test]
fn should_validate_range_bounds_correctly() {
    // Arrange
    let test_cases = vec![
        (0, 10, true),   // valid
        (10, 0, false),  // invalid
        (5, 5, false),   // invalid
    ];

    // Act & Assert
    for (start, end, expected) in test_cases {
        let result = is_valid_range(start, end);
        assert_eq!(result, expected, "Failed for ({}, {})", start, end);
    }
}
```

## Meta-Test Enforcement

**All tests are validated by `tests/test_guidelines_compliance.rs`**

The meta-test will FAIL if:

- Any test uses `test_*` naming
- Tests >5 lines are missing AAA comments
- Tests have combined AAA comments (`// Arrange & Act`)

Run it with:

```bash
cargo test test_guidelines_compliance
```

## Quick Checklist for Copilot

Before suggesting a test, verify:

- [ ] Name starts with `should_`
- [ ] If >5 lines, has `// Arrange`, `// Act`, `// Assert` (exact format)
- [ ] Only ONE `// Act` section
- [ ] Each test verifies ONE specific behavior
- [ ] Multiple assertions only if they verify facets of the SAME operation

## Examples from Codebase

See these files for excellent examples:

- `src/manifest.rs` - Clean AAA structure, proper splitting
- `src/index/range_tombstone.rs` - Single-behavior tests
- `src/cloud/mock.rs` - Upload/download properly split

## Why These Rules?

1. **Consistency**: All tests look the same → easier to read
2. **Debuggability**: One test fails → know exactly what broke
3. **Maintainability**: Change behavior → update one focused test
4. **Documentation**: Tests serve as examples of how to use code
5. **CI/CD**: Meta-test enforces rules automatically

---

**REMEMBER: When in doubt, create MORE smaller tests rather than fewer large tests!**
