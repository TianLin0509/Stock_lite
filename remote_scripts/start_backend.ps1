$ErrorActionPreference = "Stop"

$deployRoot = "C:\StockLite\app"
$pythonExe = Join-Path $deployRoot ".venv\Scripts\python.exe"
$logsDir = Join-Path $deployRoot "logs"
$stdoutLog = Join-Path $logsDir "uvicorn.stdout.log"
$stderrLog = Join-Path $logsDir "uvicorn.stderr.log"
$pidFile = Join-Path $deployRoot "uvicorn.pid"
$launcherCmd = Join-Path $deployRoot "run_backend_detached.cmd"
$taskName = "StockLiteBackend"

New-Item -ItemType Directory -Force -Path $logsDir | Out-Null

$existing = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -like "*uvicorn main:app*"
}
foreach ($proc in $existing) {
    try {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
    } catch {
    }
}

@"
@echo off
cd /d $deployRoot
"$pythonExe" -m uvicorn main:app --host 127.0.0.1 --port 8010 >> "$stdoutLog" 2>> "$stderrLog"
"@ | Set-Content -Path $launcherCmd -Encoding ASCII

cmd /c "netsh interface portproxy delete v4tov4 listenport=80 listenaddress=0.0.0.0" | Out-Null
cmd /c "netsh interface portproxy add v4tov4 listenport=80 listenaddress=0.0.0.0 connectport=8010 connectaddress=127.0.0.1" | Out-Null
cmd /c "netsh advfirewall firewall delete rule name=""StockLite HTTP 80""" | Out-Null
cmd /c "netsh advfirewall firewall add rule name=""StockLite HTTP 80"" dir=in action=allow protocol=TCP localport=80" | Out-Null

cmd /c "schtasks /Delete /TN $taskName /F" | Out-Null
cmd /c "schtasks /Create /TN $taskName /TR ""$launcherCmd"" /SC ONSTART /RU SYSTEM /RL HIGHEST /F" | Out-Null
cmd /c "schtasks /Run /TN $taskName" | Out-Null

$proc = $null
for ($i = 0; $i -lt 15; $i++) {
    Start-Sleep -Seconds 2
    $proc = Get-CimInstance Win32_Process | Where-Object {
        $_.CommandLine -like "*uvicorn main:app*" -and $_.CommandLine -like "*8010*"
    } | Select-Object -First 1

    if ($proc) {
        break
    }
}

if (-not $proc) {
    throw "uvicorn process did not start"
}

Set-Content -Path $pidFile -Value $proc.ProcessId

Write-Host "START_DONE $($proc.ProcessId)"
