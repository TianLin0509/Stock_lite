$ErrorActionPreference = "Stop"

$deployRoot = "C:\StockLite"
$bundlePath = "C:\StockLite\incoming\deploy_bundle.zip"
$extractRoot = "C:\StockLite\app"
$stageRoot = "C:\StockLite\incoming\app_stage"
$systemPython = "C:\Program Files\Python313\python.exe"
$venvPython = Join-Path $extractRoot ".venv\Scripts\python.exe"
$requirementsPath = Join-Path $extractRoot "requirements.deploy.txt"

$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"

New-Item -ItemType Directory -Force -Path $deployRoot | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $deployRoot "incoming") | Out-Null

$existing = Get-CimInstance Win32_Process | Where-Object {
    ($_.CommandLine -like "*uvicorn main:app*") -or
    ($_.CommandLine -like "*C:\StockLite\app\.venv\Scripts\python.exe*")
}
foreach ($proc in $existing) {
    try {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
    } catch {
    }
}
Start-Sleep -Seconds 2

if (Test-Path $stageRoot) {
    Remove-Item -Recurse -Force $stageRoot
}
New-Item -ItemType Directory -Force -Path $stageRoot | Out-Null

Expand-Archive -Path $bundlePath -DestinationPath $stageRoot -Force

New-Item -ItemType Directory -Force -Path $extractRoot | Out-Null

$preservePaths = @("storage", "logs", ".streamlit")
foreach ($name in $preservePaths) {
    $target = Join-Path $extractRoot $name
    if (-not (Test-Path $target)) {
        New-Item -ItemType Directory -Force -Path $target | Out-Null
    }
}

Get-ChildItem -Force $extractRoot | Where-Object {
    $_.Name -notin @("storage", "logs", ".streamlit")
} | ForEach-Object {
    Remove-Item -Recurse -Force $_.FullName
}

Get-ChildItem -Force $stageRoot | ForEach-Object {
    Move-Item -Force $_.FullName $extractRoot
}

Remove-Item -Recurse -Force $stageRoot

Set-Location $extractRoot

& $systemPython -m venv .venv
if (-not (Test-Path $requirementsPath)) {
    $requirementsPath = Join-Path $extractRoot "requirements.txt"
}

& $venvPython -m pip install --upgrade pip setuptools wheel
& $venvPython -m pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple
& $venvPython -m pip config set global.trusted-host pypi.tuna.tsinghua.edu.cn
& $venvPython -m pip install --disable-pip-version-check --prefer-binary --retries 8 --timeout 120 -r $requirementsPath

Write-Host "DEPLOY_DONE"
