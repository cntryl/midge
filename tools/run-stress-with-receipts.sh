#!/usr/bin/env bash
# Resolve and fingerprint the actual executable before its watchdog begins.
set -euo pipefail
if [[ $# != 2 ]]; then
  printf 'usage: run-stress-with-receipts.sh BENCH_TARGET WORKLOAD\n' >&2
  exit 2
fi
bench_target="$1"
selected_workload="$2"
mkdir -p target/midge-stress/runner-logs
build_receipt="target/midge-stress/runner-logs/build-${bench_target}.jsonl"
cargo bench --bench "$bench_target" --features stress-soak --no-run --message-format=json > "$build_receipt"
executable="$(jq -rs --arg target "$bench_target" '[.[] | select(.reason == "compiler-artifact" and .target.name == $target and .executable != null)] | last.executable // empty' "$build_receipt")"
if [[ ! -f "$executable" || ! -x "$executable" ]]; then
  printf 'missing measured executable for %s\n' "$bench_target" >&2
  exit 1
fi
if command -v sha256sum >/dev/null; then
  hash_line="$(sha256sum "$executable")"
else
  hash_line="$(shasum -a 256 "$executable")"
fi
export MIDGE_STRESS_BINARY_SHA256="${hash_line%% *}"
printf '%s  %s\n' "$MIDGE_STRESS_BINARY_SHA256" "$executable" > "target/midge-stress/runner-logs/binary-${bench_target}.sha256"
# Execute the exact Cargo artifact, avoiding another compile between hash/run.
"$executable" --workload "$selected_workload" --bench
