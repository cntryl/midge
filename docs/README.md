# Midge Documentation

**Central navigation hub for all Midge documentation**

_Version: 0.1.0 | Status: Production | Last updated: March 2026_

## Documentation Structure

Documentation is organized by audience:

- **[user-guides/](user-guides/)** — Getting started, API reference, troubleshooting
- **[operations/](operations/)** — Deployment, performance tuning, cloud setup
- **[development/](development/)** — Architecture, testing, contribution guidelines

## Getting Started

### New to Midge?

Start here to understand what Midge is and get up and running quickly:

1. **[Overview](user-guides/overview.md)** — What is Midge, when to use it, key features
2. **[Quick Start](user-guides/quick-start.md)** — 5-minute hello-world example
3. **[API Guide](user-guides/api-guide.md)** — Complete API reference with examples

### Common Questions

- **[FAQ](user-guides/faq.md)** — Frequently asked questions, quick answers
- **[Troubleshooting](user-guides/troubleshooting.md)** — Common issues and solutions
- **[Durability](user-guides/durability.md)** — Write guarantees and crash recovery

---

## Operating Midge in Production

For deploying, tuning, and maintaining Midge in production environments:

### Deployment

- **[Cloud Setup](operations/cloud-setup.md)** — Configure cloud storage backends (S3, GCS, Azure)
- **[Performance Tuning](operations/performance-tuning.md)** — Optimize for latency, throughput, or economy
- **[Resource Limits](operations/resource-limits.md)** — Thread management, memory limits, graceful degradation
- **[Migration Guide](operations/migration-guide.md)** — Version upgrades, backups, data migration

### Monitoring and Operations

- **[Durability Guide](user-guides/durability.md)** — Recovery guarantees by storage mode
- **[Troubleshooting](user-guides/troubleshooting.md)** — Debug performance and reliability issues

---

## Contributing to Midge

For developers working on Midge internals:

### Architecture and Design

- **[The Big Idea](development/the-big-idea.md)** — Design philosophy and architectural principles
- **[Architecture](development/architecture.md)** — Module structure, threading model, layer dependencies
- **[Recovery Internals](development/recovery-internals.md)** — WAL replay, manifest reconciliation, crash recovery

### Development Workflow

- **[Testing](development/testing.md)** — Test structure, naming conventions, validation
- **[Benchmarks](development/benchmarks.md)** — Performance benchmarking guidelines
- **[CONTRIBUTING](../CONTRIBUTING.md)** — Contribution workflow, PR process, code standards

---

## Documentation by Topic

### Storage and Durability

- [Overview - Storage Modes](user-guides/overview.md#storage-modes)
- [Durability Guarantees](user-guides/durability.md)
- [Cloud Setup](operations/cloud-setup.md)
- [Recovery Internals](development/recovery-internals.md)

### Performance

- [Performance Tuning](operations/performance-tuning.md)
- [FAQ - Performance](user-guides/faq.md#performance)
- [Benchmarks](development/benchmarks.md)
- [Troubleshooting - Performance Issues](user-guides/troubleshooting.md#performance-issues)

### API and Usage

- [Quick Start](user-guides/quick-start.md)
- [API Guide](user-guides/api-guide.md)
- [FAQ](user-guides/faq.md)
- [Troubleshooting](user-guides/troubleshooting.md)

### Architecture and Internals

- [The Big Idea](development/the-big-idea.md)
- [Architecture](development/architecture.md)
- [Recovery Internals](development/recovery-internals.md)
- [Testing](development/testing.md)

---

## Quick Links

- **GitHub Repository**: [cntryl/midge](https://github.com/cntryl/midge)
- **API Documentation** (rustdoc): Run `cargo doc --open`
- **License**: See [LICENSE](../LICENSE)
- **Security Policy**: See [SECURITY](../SECURITY.md)
- **Changelog**: See [CHANGELOG](../CHANGELOG.md)

---

## Recommended Reading Paths

### Path 1: New User (5-10 minutes)

```
Overview → Quick Start → FAQ
```

Get oriented, run first example, understand key decisions.

### Path 2: Production Deployment (30-60 minutes)

```
Overview → API Guide → Durability → Performance Tuning → Cloud Setup
```

Understand features, configure durability, tune for workload, deploy to cloud.

### Path 3: Contributor (1-2 hours)

```
The Big Idea → Architecture → Testing → Recovery Internals → CONTRIBUTING
```

Understand design philosophy, code structure, test conventions, and contribution workflow.

---

## Documentation Conventions

### File Naming

- Lowercase with hyphens: `api-guide.md`, `cloud-setup.md`
- Descriptive names matching content

### Cross-References

- Relative paths from current location
- Example: `../operations/cloud-setup.md`

### Code Examples

- All examples are runnable (unless marked `no_run`)
- Examples use realistic data and patterns
- Examples demonstrate error handling

### Professional Standards

- Clear technical writing, no jargon without definition
- Minimal emoji use (only for status tables: ✅/❌/⚠️)
- All features documented are implemented and tested

---

## Contributing to Documentation

Documentation contributions are welcome! When updating docs:

1. **Keep audience in mind**: User guides for users, development docs for contributors
2. **Test code examples**: Ensure all examples compile and run
3. **Update cross-references**: Check links after moving/renaming files
4. **Follow conventions**: Lowercase filenames, relative paths, minimal emoji

See [CONTRIBUTING.md](../CONTRIBUTING.md) for general contribution guidelines.

---

## Help and Support

- **Documentation issues**: File issue on GitHub
- **Questions**: Check [FAQ](user-guides/faq.md) first
- **Bugs**: File issue with reproduction steps
- **Features**: Discuss in GitHub issues before PR

---

**Last updated**: February 2026
