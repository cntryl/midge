Absolutely — here’s your complete, polished version of the **GitHub Copilot Instructions for the Midge Project**, with consistent tone, typography, and structure, plus the new **Automation Scripts** section added cleanly at the top.

# GitHub Copilot Instructions for Midge Project

## Automation Scripts

When writing automation for this project, **prefer Python over PowerShell**.

- **Python** offers better **cross-platform compatibility**, **library support**, and **VS Code integration**.
- **PowerShell** is acceptable for quick Windows-specific admin or shell tasks, but all **project automation (build helpers, generators, CI utilities)** must be written in **Python** for consistency and maintainability.
- Use `.py` scripts in the `/tools` directory when adding new automation logic.

## Test Writing Guidelines — STRICTLY ENFORCE

When generating or suggesting tests, **ALWAYS** follow these rules.

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

Every test **must** have exactly these three comments:

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
- ❌ `// Setup` (use `// Arrange`)
- ❌ Descriptive comments like `// Arrange - create database`

**ALWAYS use:**

- ✅ Exactly `// Arrange`
- ✅ Exactly `// Act`
- ✅ Exactly `// Assert`

### 3. Single Behavior Principle (PEDANTIC RULE)

Each test must verify **one behavior only**.
If each `assert_eq!` tests a different input/output pair, **split into multiple tests**.

```rust
// ❌ WRONG - Testing multiple inputs
#[test]
fn should_return_files_at_level() {
    let l0 = manifest.files_at_level(0);
    let l1 = manifest.files_at_level(1);
    let l2 = manifest.files_at_level(2);
    assert_eq!(l0.len(), 1);
    assert_eq!(l1.len(), 2);
    assert_eq!(l2.len(), 0);
}

// ✅ CORRECT - Focused, one behavior per test
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

**Exception:**
Multiple assertions verifying **facets of the same operation** are acceptable:

```rust
// ✅ CORRECT - All assertions validate one property
#[test]
fn should_preserve_data_across_save_load() {
    // Arrange
    let original = create_manifest();

    // Act
    let loaded = save_and_load(original);

    // Assert
    assert_eq!(loaded.id, original.id);
    assert_eq!(loaded.name, original.name);
    assert_eq!(loaded.size, original.size);
}
```

### 4. No Multiple Act Sections

**Never** have more than one `// Act` section per test.

```rust
// ❌ WRONG
#[test]
fn should_upload_and_download() {
    // Arrange
    let backend = Backend::new();

    // Act
    backend.upload("data");

    // Assert
    assert_eq!(backend.count(), 1);

    // Act  // ❌ SECOND ACT - WRONG
    let downloaded = backend.download();

    // Assert
    assert_eq!(downloaded, "data");
}

// ✅ CORRECT
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

Tests with ≤5 lines may omit AAA comments, but still require correct naming.

```rust
// ✅ CORRECT - Short test, AAA optional
#[test]
fn should_create_default_config() {
    let config = Config::default();
    assert_eq!(config.timeout, 30);
}
```

## Common Patterns

### Testing Serialization/Deserialization

Always **separate** serialization and deserialization tests.

```rust
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

Each scenario must have its own test.

```rust
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

Use only when verifying the same logic with multiple inputs.

```rust
#[test]
fn should_validate_range_bounds_correctly() {
    // Arrange
    let test_cases = vec![
        (0, 10, true),
        (10, 0, false),
        (5, 5, false),
    ];

    // Act & Assert
    for (start, end, expected) in test_cases {
        let result = is_valid_range(start, end);
        assert_eq!(result, expected, "Failed for ({}, {})", start, end);
    }
}
```

## Meta-Test Enforcement

Compliance is validated by `tests/test_guidelines_compliance.rs`.
The meta-test will **fail** if:

- Any test uses `test_*` naming
- Tests >5 lines are missing AAA comments
- Combined AAA comments (`// Arrange & Act`, etc.) exist

Run manually with:

```bash
cargo test test_guidelines_compliance
```

## Quick Checklist for Copilot

Before suggesting a test, verify:

- [ ] Name starts with `should_`
- [ ] If >5 lines, includes `// Arrange`, `// Act`, `// Assert`
- [ ] Only **one** `// Act` section
- [ ] Verifies **one** behavior per test
- [ ] Multiple assertions only if validating **facets of one operation**

## Example References

Excellent examples can be found in:

- `src/manifest.rs` — Clean AAA structure
- `src/index/range_tombstone.rs` — Single-behavior tests
- `src/cloud/mock.rs` — Upload/download split properly

## Why These Rules Exist

1. **Consistency** — Tests all follow the same readable pattern.
2. **Debuggability** — A failing test pinpoints the exact behavior.
3. **Maintainability** — Code changes require minimal test churn.
4. **Documentation** — Tests serve as living usage examples.
5. **CI Enforceability** — The meta-test automatically guards rules.

**REMEMBER:** When in doubt, create **more smaller tests**, not fewer large ones.
Clarity beats cleverness.
