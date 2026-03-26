$ErrorActionPreference = "Stop"

$deployRoot = "C:\StockLite\app"
$pythonExe = Join-Path $deployRoot ".venv\Scripts\python.exe"

Set-Location $deployRoot
& $pythonExe -m uvicorn main:app --host 127.0.0.1 --port 8001
