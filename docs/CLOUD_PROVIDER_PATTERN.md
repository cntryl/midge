# Cloud Provider Implementation Pattern

This document defines the architectural pattern for implementing cloud storage providers in Midge. All custom cloud provider implementations must follow this pattern to ensure consistency, testability, and maintainability.

## Architecture Overview

### Core Pattern
```
Application (Engine)
        ↓
  CloudCallback Interface (sync channels)
        ↓
  CloudProvider (S3, GCS, Azure, OCI)
        ↓
  Direct REST API (no heavy SDKs)
        ↓
  HTTP Client (ureq for sync, reqwest for async)
```

**Key Principle:** Zero async contamination in the engine core. All I/O operations use callback-based communication with sync channels.

## Provider Interface

All cloud providers must implement these four operations:

### 1. PUT Operation
```rust
pub fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback)
```
- **Purpose:** Upload object to cloud storage
- **Inputs:** Object key and data blob
- **Callback Event:** `CloudEvent::PutComplete { key, result }`
- **Result:** `CloudOutcome::Ok(())` on success, `CloudOutcome::Error(msg)` on failure

### 2. GET Operation
```rust
pub fn submit_get(&self, key: String, callback: CloudCallback)
```
- **Purpose:** Download object from cloud storage
- **Inputs:** Object key
- **Callback Event:** `CloudEvent::GetComplete { key, result }`
- **Result:** `CloudOutcome::Ok(Vec<u8>)` containing data on success, `CloudOutcome::Error(msg)` on failure

### 3. DELETE Operation
```rust
pub fn submit_delete(&self, key: String, callback: CloudCallback)
```
- **Purpose:** Delete object from cloud storage
- **Inputs:** Object key
- **Callback Event:** `CloudEvent::DeleteComplete { key, result }`
- **Result:** `CloudOutcome::Ok(())` on success, `CloudOutcome::Error(msg)` on failure

### 4. LIST Operation
```rust
pub fn submit_list(&self, prefix: String, callback: CloudCallback)
```
- **Purpose:** List objects matching a prefix
- **Inputs:** Object key prefix
- **Callback Event:** `CloudEvent::ListComplete { prefix, result }`
- **Result:** `CloudOutcome::Ok(Vec<String>)` containing object keys on success, `CloudOutcome::Error(msg)` on failure

## Implementation Checklist

For each cloud provider (S3, GCS, Azure, OCI), implement:

### Basic Structure
- [ ] Provider struct with necessary credentials/config fields
- [ ] Constructor that takes provider-specific configuration
- [ ] Four operation methods (PUT, GET, DELETE, LIST)
- [ ] All methods use callback-based communication
- [ ] All methods must be non-blocking (spawn async/background tasks as needed)

### Authentication
- [ ] **S3:** SigV4 request signing (AWS signature version 4)
  - HMAC-SHA256 signatures for request authentication
  - Request signing follows AWS specification
  
- [ ] **GCS:** OAuth 2.0 with service account
  - JWT-based authentication
  - Signed tokens for API requests
  
- [ ] **Azure:** SAS token or shared key authentication
  - Signature calculation with account key
  - Shared key format for Azure Storage API
  
- [ ] **OCI:** Signature-based authentication
  - RSA signature with private key
  - Custom OCI authentication headers

### HTTP Operations
- [ ] Construct proper REST API endpoints for each operation
- [ ] Set required authentication headers
- [ ] Handle HTTP status codes and errors
- [ ] Parse response bodies correctly
- [ ] Send CloudEvent via callback upon completion

### Error Handling
- [ ] Network errors → `CloudOutcome::Error(message)`
- [ ] Authentication failures → `CloudOutcome::Error(message)`
- [ ] Invalid responses → `CloudOutcome::Error(message)`
- [ ] All errors sent via callback (no panics)

### Testing Requirements

Each provider must have comprehensive tests:

**Provider Creation Tests (1 per provider)**
```rust
#[test]
fn should_create_{provider}_provider()
```

**Operation Tests (4 per provider)**
```rust
#[test]
fn should_submit_put_operation()
fn should_submit_get_operation()
fn should_submit_delete_operation()
fn should_submit_list_operation()
```

**Error Handling Tests (2 per provider)**
```rust
#[test]
fn should_handle_auth_failure()
fn should_handle_network_error()
```

**Total: ~7-8 tests per provider = 28-32 tests**

## Provider-Specific Implementation Notes

### S3 (Amazon Web Services)
- **Endpoint:** `https://s3.{region}.amazonaws.com/{bucket}/{key}`
- **Auth Header:** `Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...`
- **Required Headers:** 
  - Host
  - Date
  - Authorization
  - Content-Length (for PUT)
- **Useful Crates:** `hmac`, `sha2`, `chrono` (already in Cargo.toml)

### GCS (Google Cloud Storage)
- **Endpoint:** `https://www.googleapis.com/storage/v1/b/{bucket}/o/{key}`
- **Auth Header:** `Authorization: Bearer {access_token}`
- **Token Type:** JWT signed by service account
- **Required Headers:**
  - Authorization
  - Content-Type
- **Useful Crates:** `serde_json`, `base64` (already in Cargo.toml)

### Azure Blob Storage
- **Endpoint:** `https://{account}.blob.core.windows.net/{container}/{blob}`
- **Auth Header:** `Authorization: SharedKey {account}:{signature}`
- **Signature:** Base64(HMAC-SHA256(key, string_to_sign))
- **Required Headers:**
  - Authorization
  - x-ms-version
  - x-ms-date
  - Content-Length
- **Useful Crates:** `hmac`, `sha2`, `base64` (already in Cargo.toml)

### OCI (Oracle Cloud Infrastructure)
- **Endpoint:** `https://objectstorage.{region}.oraclecloud.com/n/{namespace}/b/{bucket}/o/{object}`
- **Auth Header:** `Authorization: Signature version=1, keyId={keyId}, algorithm="rsa-sha256", signature="{signature}"`
- **Signature:** RSA-SHA256 signature of canonical request string
- **Required Headers:**
  - Authorization
  - date
- **Useful Crates:** Custom signature generation with standard crypto libraries

## Current Status

- [x] CloudCallback interface defined
- [x] CloudEvent enum with all operation variants
- [x] CloudOutcome wrapper for Clone-safe results
- [x] MockCloud provider for testing
- [x] Provider stubs scaffolded (S3, GCS, Azure, OCI)
- [ ] S3 implementation
- [ ] GCS implementation
- [ ] Azure implementation
- [ ] OCI implementation

## Next Steps

1. **Implement one provider end-to-end** (recommended: S3) to validate pattern
2. **Create shared authentication utilities** (SigV4 signer, etc.)
3. **Implement remaining providers** following the same pattern
4. **Integration tests** with real cloud services (optional)
5. **Performance benchmarks** for cloud I/O operations

## Example: Complete Provider Template

```rust
//! {Provider} Cloud Storage Provider
//!
//! Implements callback-based I/O for {Provider} object storage.
//! All operations use sync channels and spawn background tasks as needed.

use crate::storage::cloud::{CloudCallback, CloudEvent, CloudOutcome};

/// {Provider} provider implementation
pub struct {Provider}Provider {
    // Configuration fields
    config_field1: String,
    config_field2: String,
    // credentials: Credentials,
}

impl {Provider}Provider {
    /// Create a new {Provider} provider
    pub fn new(config_field1: String, config_field2: String) -> Self {
        Self {
            config_field1,
            config_field2,
        }
    }

    /// Submit a PUT operation
    pub fn submit_put(&self, key: String, data: Vec<u8>, callback: CloudCallback) {
        // 1. Build request (URL, headers, body)
        // 2. Sign request (authentication)
        // 3. Send HTTP request (blocking or async)
        // 4. Parse response
        // 5. Send callback event
        // 6. Return immediately (no blocking in main thread)
        
        let event = CloudEvent::PutComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    /// Submit a GET operation
    pub fn submit_get(&self, key: String, callback: CloudCallback) {
        // Similar pattern to PUT
        let event = CloudEvent::GetComplete {
            key,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }

    /// Submit a DELETE operation
    pub fn submit_delete(&self, key: String, callback: CloudCallback) {
        // Similar pattern to PUT
        let event = CloudEvent::DeleteComplete {
            key,
            result: CloudOutcome::Ok(()),
        };
        let _ = callback.send(event);
    }

    /// Submit a LIST operation
    pub fn submit_list(&self, prefix: String, callback: CloudCallback) {
        // Similar pattern to GET
        let event = CloudEvent::ListComplete {
            prefix,
            result: CloudOutcome::Ok(Vec::new()),
        };
        let _ = callback.send(event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_provider() {
        let provider = {Provider}Provider::new(
            "config1".to_string(),
            "config2".to_string(),
        );
        assert_eq!(provider.config_field1, "config1");
    }
}
```

## Related Files

- Implementation stubs: `src/storage/providers/{s3,gcs,azure,oci}.rs`
- Cloud callback interface: `src/storage/cloud.rs`
- Storage backend trait: `src/storage/mod.rs`
- Integration with engine: `src/runtime/actors/cloud.rs`
