#!/usr/bin/env bash
# After-run capture only. No benchmark, Cargo, Python or mutation of prior input.
# Raw REST origin, complete pagination, exact provider archives and canonical
# receipts are read by the Rust executable; no summary is substituted for them.
set -euo pipefail
if test "$#" -ne 8; then
  echo 'usage: MODE REPOSITORY RUN_ID ATTEMPT SHA FRESH_DIRECTORY READER OUTPUT_REPORT' >&2
  exit 2
fi
checkpoint_mode="$1"
checkpoint_repository="$2"
checkpoint_run="$3"
checkpoint_attempt="$4"
checkpoint_sha="$5"
checkpoint_root="$6"
checkpoint_reader="$7"
checkpoint_report="$8"
case "$checkpoint_mode" in construction-smoke|readback) ;; *) exit 2 ;; esac
[[ "$checkpoint_repository" =~ ^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$ ]]
[[ "$checkpoint_run" =~ ^[1-9][0-9]*$ ]]
[[ "$checkpoint_attempt" =~ ^[1-9][0-9]*$ ]]
[[ "$checkpoint_sha" =~ ^[[:xdigit:]]{40}$ ]]
test -x "$checkpoint_reader"
# Never reuse the input or overwrite an earlier readback report.
mkdir "$checkpoint_root"
mkdir "$checkpoint_root/hosted"
test ! -e "$checkpoint_report"
case "$checkpoint_report" in "$checkpoint_root"/*) exit 2 ;; esac

# Even a transport/capture interruption must retain a nine-row invalid report.
# Preserve the original command's exit status and never replace an old report.
checkpoint_preserve_report() {
  checkpoint_original_status="$?"
  trap - EXIT
  if test ! -e "$checkpoint_report"; then
    set +e
    "$checkpoint_reader" "$checkpoint_mode" --root "$checkpoint_root" --run-id "$checkpoint_run" --attempt "$checkpoint_attempt" --sha "$checkpoint_sha" > "$checkpoint_report"
  fi
  exit "$checkpoint_original_status"
}
trap checkpoint_preserve_report EXIT

gh api "repos/$checkpoint_repository/actions/runs/$checkpoint_run/attempts/$checkpoint_attempt" > "$checkpoint_root/hosted/run.json"
gh api --paginate --slurp "repos/$checkpoint_repository/actions/runs/$checkpoint_run/attempts/$checkpoint_attempt/jobs?per_page=100" | jq '{total_count:.[0].total_count,jobs:[.[].jobs[]]}' > "$checkpoint_root/hosted/jobs.json"
gh api --paginate --slurp "repos/$checkpoint_repository/actions/runs/$checkpoint_run/artifacts?per_page=100" | jq '{total_count:.[0].total_count,artifacts:[.[].artifacts[]]}' > "$checkpoint_root/hosted/artifacts.json"

"$checkpoint_reader" prepare-download --root "$checkpoint_root" --run-id "$checkpoint_run" --attempt "$checkpoint_attempt" --sha "$checkpoint_sha"
mkdir "$checkpoint_root/archives" "$checkpoint_root/artifacts"
# The already-created originating receipt contains only the expected exact names.
while IFS=$'\t' read -r checkpoint_id checkpoint_name; do
  [[ "$checkpoint_id" =~ ^[1-9][0-9]*$ ]]
  [[ "$checkpoint_name" =~ ^[A-Za-z0-9_-]+$ ]]
  checkpoint_archive="$checkpoint_root/archives/$checkpoint_id.zip"
  gh api "repos/$checkpoint_repository/actions/artifacts/$checkpoint_id/zip" > "$checkpoint_archive"
  # Refuse unsafe/literal-pattern member names before extracting into a fresh
  # evidence directory. The Rust seal independently checks the exact file set
  # and provider archive digest/size as well as every member/local byte hash.
  unzip -Z1 "$checkpoint_archive" | awk '/(^\/|(^|\/)\.\.?(\/|$)|[*?\[\]\\])/ {bad=1} END {exit bad}'
  mkdir "$checkpoint_root/artifacts/$checkpoint_name"
  unzip -q "$checkpoint_archive" -d "$checkpoint_root/artifacts/$checkpoint_name"
done < <(jq -r '.artifacts[] | [.id,.name] | @tsv' "$checkpoint_root/download-provenance.json")
"$checkpoint_reader" seal-download --root "$checkpoint_root" --run-id "$checkpoint_run" --attempt "$checkpoint_attempt" --sha "$checkpoint_sha"
# Preserve the exact returned report even on qualification failure.
"$checkpoint_reader" "$checkpoint_mode" --root "$checkpoint_root" --run-id "$checkpoint_run" --attempt "$checkpoint_attempt" --sha "$checkpoint_sha" > "$checkpoint_report"
