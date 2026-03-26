param(
    [Parameter(Mandatory = $true)]
    [string]$OpenId
)

$ErrorActionPreference = "Stop"

$taskName = "StockLiteTop10Recovery"
$launcher = "C:\StockLite\app\remote_scripts\run_top10_notify_once.cmd $OpenId"

cmd /c "schtasks /Delete /TN $taskName /F" | Out-Null
cmd /c "schtasks /Create /TN $taskName /TR ""$launcher"" /SC ONCE /ST 23:59 /RU SYSTEM /RL HIGHEST /F" | Out-Null
cmd /c "schtasks /Run /TN $taskName" | Out-Null

Write-Host "RECOVERY_TASK_STARTED $taskName"
