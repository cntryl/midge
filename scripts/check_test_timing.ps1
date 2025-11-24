# PowerShell script to detect potentially flaky timing patterns in tests
# Usage: run from repo root: pwsh -File scripts/check_test_timing.ps1

$patterns = @(
    "std::thread::sleep\(",
    "\bsleep\(", # generic
    "Duration::from_millis\(",
    "Duration::from_secs\(",
    "\.recv\(\)",
    "\.recv\(\)\.unwrap\(",
    "\.recv\(\)\.expect\(",
    "wait_for_compaction\(",
    "wait_for_flush\("
)

# Files or patterns to ignore (whitelist) because they intentionally use timing/gates
$fileWhitelist = @(
    'test_hooks_integration.rs',
    'snapshot_lifecycle_compaction.rs',
    'multicf_compaction_recovery.rs',
    'range_delete_edge_cases.rs',
    'durability_compaction.rs',
    'engine_compaction.rs',
    'memtable_concurrency.rs'
    'test_helpers.rs'
)

# Obtain repo root (parent directory of scripts directory)
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..')
Push-Location $repoRoot

$files = Get-ChildItem -Recurse -Include "*.rs" -Path .\tests | Select-Object -ExpandProperty FullName

$matches = @()
foreach ($f in $files) {
    foreach ($p in $patterns) {
        $res = Select-String -Path $f -Pattern $p -ErrorAction SilentlyContinue
        if ($res) {
            foreach ($m in $res) {
                # Skip matches in whitelisted files
                $fileName = [System.IO.Path]::GetFileName($f)
                if ($fileWhitelist -contains $fileName) {
                    continue
                }
                $matches += [PSCustomObject]@{ File = $f; Line = $m.LineNumber; Pattern = $p; Text = $m.Line }
            }
        }
    }
}

if ($matches.Count -eq 0) {
    Write-Host "No timing-related patterns found in test files."
    Pop-Location
    exit 0
}

Write-Host "Found potential timing-related patterns in test files:" -ForegroundColor Yellow
$matches | Sort-Object File, Line | Format-Table -AutoSize

# Make this script fail (non-zero) to integrate in CI and require review
Write-Host "Please review the above occurrences and convert to deterministic patterns where applicable." -ForegroundColor Red
Pop-Location
exit 1
