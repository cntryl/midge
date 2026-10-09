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
if [[ "${MIDGE_STRESS_CPU_PROFILE:-0}" == "1" ]]; then
  # Keep the workload under the runner identity; only the sampler is privileged.
  profile_environment=()
  while IFS= read -r -d '' variable; do
    case "$variable" in
      MIDGE_*|STRESS_*|SQRZL_*|TMPDIR=*|RUST_LOG=*) profile_environment+=("$variable") ;;
    esac
  done < <(env -0)
  profile_user="$(id -un)"
  set +e
  sudo -n perf record -F 99 -g --call-graph dwarf \
    -o target/midge-stress/runner-logs/perf.data -- \
    runuser -u "$profile_user" -- env "${profile_environment[@]}" \
      "$executable" --workload "$selected_workload" --bench
  profile_result=$?
  set -e
  if [[ -f target/midge-stress/runner-logs/perf.data ]]; then
    sudo -n chown "$(id -u):$(id -g)" target/midge-stress/runner-logs/perf.data
  fi
  exit "$profile_result"
else
  "$executable" --workload "$selected_workload" --bench
fi
