# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue in Midge, please report it responsibly.

### How to Report

**DO NOT** open a public GitHub issue for security vulnerabilities.

Instead, please email security concerns to: **security@cntryl.com** (or the maintainer email listed in Cargo.toml)

Include in your report:
- Description of the vulnerability
- Steps to reproduce
- Affected versions
- Potential impact
- Any suggested fixes

### What to Expect

- **Initial Response**: We aim to acknowledge your report within 48 hours
- **Status Updates**: We will keep you informed of our progress
- **Disclosure Timeline**: We aim to release fixes within 90 days
- **Credit**: We will credit you in the security advisory (unless you prefer to remain anonymous)

### Security Best Practices for Users

1. **Keep Updated**: Always use the latest version of Midge
2. **Least Privilege**: Run Midge processes with minimal required permissions
3. **Cloud Credentials**: Store cloud credentials securely (use environment variables, never commit to git)
4. **File Permissions**: Set restrictive permissions on database directories (0600 for files, 0700 for directories)
5. **Encryption**: Use encryption at rest for sensitive data (filesystem-level or cloud-provider encryption)
6. **Network**: Cloud mode transmits data to object storage - ensure TLS is enabled on cloud providers

### Known Limitations

- **Single-Writer**: Midge enforces single-writer via filesystem/cloud leases. Bypassing this can cause data corruption.
- **No Built-in Encryption**: Midge does not encrypt data at the application layer. Use filesystem or cloud encryption.
- **Lease Availability**: Cloud mode depends on lease availability. Ensure your cloud setup meets durability SLAs.

### Security Features

- Checksums (CRC32C) on all data blocks and records
- Corruption detection on reads
- WAL integrity validation on recovery
- Exclusive write access via leases
- No remote code execution surface (embedded library, not a server)
