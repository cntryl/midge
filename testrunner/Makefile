.PHONY: all build test bench stress clean help

# Timeout values in seconds
BUILD_TIMEOUT := 600      # 10 minutes
TEST_TIMEOUT := 1200      # 20 minutes
BENCH_TIMEOUT := 1800     # 30 minutes
STRESS_TIMEOUT := 1200    # 20 minutes

all: build test bench stress
	@echo "✓ All stages completed successfully!"

build:
	@echo "=== Building Midge (timeout: $(BUILD_TIMEOUT)s) ==="
	timeout $(BUILD_TIMEOUT) cargo build --release --workspace || { echo "✗ Build timed out or failed"; exit 1; }
	@echo "✓ Build completed"

test: build
	@echo "=== Running tests (timeout: $(TEST_TIMEOUT)s) ==="
	mkdir -p /output
	timeout $(TEST_TIMEOUT) cargo test --release 2>&1 | tee /output/test_results.log || { echo "✗ Tests timed out or failed"; exit 1; }
	@echo "✓ Tests completed"

bench: test
	@echo "=== Running benchmarks (timeout: $(BENCH_TIMEOUT)s) ==="
	mkdir -p /output/criterion
	timeout $(BENCH_TIMEOUT) cargo bench --release --bench 'tier1_*' --bench 'tier2_*' --bench 'tier3_*' --bench 'tier4_*' 2>&1 | tee /output/bench_results.log || { echo "⚠ Benchmarks timed out or failed (continuing to stress tests)"; }
	if [ -d target/criterion ]; then cp -r target/criterion/* /output/criterion/ 2>/dev/null || true; fi
	@echo "✓ Benchmarks completed"

stress: bench
	@echo "=== Running stress tests (timeout: $(STRESS_TIMEOUT)s) ==="
	mkdir -p /output
	timeout $(STRESS_TIMEOUT) cargo stress -v 2>&1 | tee /output/stress.log || { echo "✗ Stress tests timed out or failed"; exit 1; }
	@echo "✓ Stress tests completed"

clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	rm -rf /output/*

help:
	@echo "Midge Test Runner Makefile"
	@echo ""
	@echo "Targets:"
	@echo "  all     - Run full pipeline: build → test → bench → stress (default)"
	@echo "  build   - Compile Midge in release mode (10m timeout)"
	@echo "  test    - Run unit and integration tests (20m timeout)"
	@echo "  bench   - Run Tier 1-4 benchmarks (30m timeout)"
	@echo "  stress  - Run stress tests (20m timeout)"
	@echo "  clean   - Remove build artifacts"
	@echo "  help    - Show this help message"
	@echo ""
	@echo "Results are saved to /output/"
