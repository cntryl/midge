# Bench Dockerfile for Midge
# - Builds with Rust 1.91 (matches rust-version in Cargo.toml)
# - Runs Criterion benches via `cargo bench`
# - Supports optional features and bench target filtering via env vars
#
# Usage (examples shown for Windows PowerShell):
#   # Build image
#   #   docker build -f .docker/bench/Dockerfile -t midge-bench .
#
#   # Run all benches; artifacts will be written next to this Dockerfile under
#   #   .docker/bench/target/criterion/... and text logs under .docker/bench/results
#   #   docker run --rm -v ${PWD}:/app -w /app midge-bench
#
#   # Run a specific bench target (as declared in Cargo.toml)
#   #   docker run --rm -e BENCH_TARGET=hotpath_api -v ${PWD}:/app -w /app midge-bench
#
#   # Enable optional features (e.g., uring, cloud-*). Multiple features separate by space
#   #   docker run --rm -e FEATURES="uring" -v ${PWD}:/app -w /app midge-bench
#
#   # Pass additional Criterion args (after `--`)
#   #   docker run --rm -e BENCH_ARGS="--sample-size 50" -v ${PWD}:/app -w /app midge-bench

FROM rust:1.91-slim AS bench

# Install basic build tooling. Keep slim.
RUN apt-get update \
	&& DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
		ca-certificates \
		build-essential \
		pkg-config \
		git \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Ensure all build and Criterion outputs land next to this Dockerfile on host
ENV CARGO_TARGET_DIR=/app/.docker/bench/target

# Pre-fetch dependencies for better layer caching
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
	&& echo "fn main() {}" > src/main.rs \
	&& cargo fetch \
	&& cargo build --release --bins || true

# Bring in the full workspace (examples, benches, src, tests, etc.)
COPY . .

# Common env knobs
ENV CARGO_TERM_COLOR=always \
	RUSTFLAGS="-C debuginfo=1" \
	FEATURES=""

# Wrapper: run all benches sequentially, write stdout/stderr to files
RUN cat > /usr/local/bin/run_benches <<'EOF' \
	&& sed -i 's/\r$//' /usr/local/bin/run_benches \
	&& chmod +x /usr/local/bin/run_benches
#!/bin/sh
set -eu

RESULT_DIR="/app/.docker/bench/results"
mkdir -p "$RESULT_DIR"

FEATURES_FLAG=""
if [ -n "${FEATURES:-}" ]; then
	FEATURES_FLAG="--features $FEATURES"
fi

BENCHES="hotpath_api hotpath_storage hotpath_index hotpath_wal hotpath_cache hotpath_sst hotpath_tlv hotpath_overhead_analysis subsystem_engine subsystem_concurrency_compaction system_compaction ycsb_workload_a ycsb_workload_b ycsb_workload_c ycsb_workload_d"

FAIL=0
for B in $BENCHES; do
	echo "[midge-bench] Running: cargo bench --bench $B $FEATURES_FLAG"
	if cargo bench --bench "$B" $FEATURES_FLAG > "$RESULT_DIR/$B.txt" 2>&1; then
		echo "[midge-bench] SUCCESS: $B"
	else
		echo "[midge-bench] FAILED: $B (see $RESULT_DIR/$B.txt)"
		FAIL=1
	fi
done

exit $FAIL
EOF

# Default command runs all benches
CMD ["/usr/local/bin/run_benches"]

