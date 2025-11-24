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

# Obtain repo root (parent directory of scripts directory)
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir '..')
Push-Location $repoRoot

$files = Get-ChildItem -Recurse -Include "*.rs" -Path .\tests | Select-Object -ExpandProperty FullName

$matches = @()
foreach ($f in $files) {
    foreach ($p in $patterns) {
        $res = Select-String -Path $f -Pattern $p -SimpleMatch -ErrorAction SilentlyContinue
        if ($res) {
            foreach ($m in $res) {
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
