$ErrorActionPreference = "Continue"

$deployRoot = "C:\StockLite\app"
$logsDir = Join-Path $deployRoot "logs"
$pidFile = Join-Path $deployRoot "uvicorn.pid"
$stdoutLog = Join-Path $logsDir "uvicorn.stdout.log"
$stderrLog = Join-Path $logsDir "uvicorn.stderr.log"

Write-Host "=== PID ==="
if (Test-Path $pidFile) {
    $savedPid = (Get-Content $pidFile | Select-Object -First 1).Trim()
    Write-Host $savedPid
    try {
        Get-Process -Id ([int]$savedPid) | Select-Object Id, ProcessName, StartTime
    } catch {
        Write-Host "PID_NOT_RUNNING"
    }
} else {
    Write-Host "PID_FILE_MISSING"
}

Write-Host "=== PORT 80 ==="
Get-NetTCPConnection -LocalPort 80 -ErrorAction SilentlyContinue |
    Select-Object LocalAddress, LocalPort, State, OwningProcess

Write-Host "=== STDERR ==="
if (Test-Path $stderrLog) {
    Get-Content $stderrLog -Tail 120
} else {
    Write-Host "STDERR_MISSING"
}

Write-Host "=== STDOUT ==="
if (Test-Path $stdoutLog) {
    Get-Content $stdoutLog -Tail 120
} else {
    Write-Host "STDOUT_MISSING"
}
