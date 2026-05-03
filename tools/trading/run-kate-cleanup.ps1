# Kate stale-bracket cleanup runner — 2026-05-03
#
# SCPs the cleanup SQL to the Kate Windows VPS, executes it against
# state.db via sqlite3, captures pre/post output for review, and
# returns the captured transcript path.
#
# Usage:
#   pwsh tools/trading/run-kate-cleanup.ps1 [-DryRun]
#
# DryRun mode runs only the read-only verification queries (the bits
# outside BEGIN TRANSACTION...COMMIT). Use this first to confirm the
# pre-state matches Gemini's diagnosis before authorising the real run.
#
# Approval class: APPROVAL — wraps destructive production state mutation.
# Do not run without explicit CEO sign-off.

[CmdletBinding()]
param(
    [switch]$DryRun,
    [string]$VpsHost = "Administrator@149.102.150.132",
    [string]$RemoteStateDb = "C:\kate\state.db",
    [string]$RemoteScriptPath = "C:\kate\cleanup-2026-05-03.sql"
)

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
$localSql = Join-Path $here "clear-stale-pending-orders.sql"
$transcriptDir = Join-Path $here "..\..\logs\cleanup"
New-Item -ItemType Directory -Path $transcriptDir -Force | Out-Null
$ts = Get-Date -Format "yyyyMMdd-HHmmss"
$transcript = Join-Path $transcriptDir "kate-cleanup-$ts.txt"

if (-not (Test-Path $localSql)) {
    throw "Cleanup SQL not found at $localSql"
}

Write-Host ""
Write-Host "Kate cleanup — $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Cyan
Write-Host "  Mode:        $(if ($DryRun) { 'DRY-RUN (read-only)' } else { 'LIVE (will mutate state.db)' })"
Write-Host "  VPS host:    $VpsHost"
Write-Host "  state.db:    $RemoteStateDb"
Write-Host "  Local SQL:   $localSql"
Write-Host "  Transcript:  $transcript"
Write-Host ""

if (-not $DryRun) {
    Write-Host "WARNING — about to run destructive cleanup on production state.db." -ForegroundColor Yellow
    Write-Host "Backup table 'orders_backup_2026_05_03' will be created BEFORE the DELETE."
    Write-Host "Press Enter to continue, Ctrl-C to abort." -ForegroundColor Yellow
    [void](Read-Host)
}

# Step 1 — SCP the SQL to the VPS
Write-Host "[1/3] SCP cleanup SQL to VPS..." -ForegroundColor Green
scp $localSql "${VpsHost}:${RemoteScriptPath}"
if ($LASTEXITCODE -ne 0) { throw "SCP failed (exit $LASTEXITCODE)" }

# Step 2 — Run the SQL via sqlite3 on the VPS
Write-Host "[2/3] Executing on VPS..." -ForegroundColor Green
if ($DryRun) {
    # Dry-run: extract only the pre-cleanup verification queries
    $dryRunSql = "C:\kate\cleanup-2026-05-03-dryrun.sql"
    ssh $VpsHost @"
powershell -Command "Get-Content '$RemoteScriptPath' | Select-String -Pattern 'BEGIN TRANSACTION' -Context 0,0 -SimpleMatch -List | ForEach-Object { (Get-Content '$RemoteScriptPath')[0..(`$_.LineNumber - 2)] | Set-Content '$dryRunSql' }"
"@
    ssh $VpsHost "sqlite3 `"$RemoteStateDb`" `".read $dryRunSql`"" | Tee-Object -FilePath $transcript
} else {
    ssh $VpsHost "sqlite3 `"$RemoteStateDb`" `".read $RemoteScriptPath`"" | Tee-Object -FilePath $transcript
}
if ($LASTEXITCODE -ne 0) { throw "sqlite3 execution failed (exit $LASTEXITCODE)" }

# Step 3 — Report
Write-Host ""
Write-Host "[3/3] Done." -ForegroundColor Green
Write-Host "Transcript saved to: $transcript"
Write-Host ""
if (-not $DryRun) {
    Write-Host "Next steps:"
    Write-Host "  1. Review transcript — confirm 'remaining stale PENDING' = 0 and kill_switch.state = INACTIVE"
    Write-Host "  2. Visit kate.theomniengine.com — confirm drift count drops, kill-switch banner clears"
    Write-Host "  3. Restart Kate ONLY after CEO confirms paper-validation may resume"
}
