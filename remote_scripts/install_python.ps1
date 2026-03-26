$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$installer = Join-Path $env:TEMP "python-3.13.2-amd64.exe"
Invoke-WebRequest "https://www.python.org/ftp/python/3.13.2/python-3.13.2-amd64.exe" -OutFile $installer
Start-Process -FilePath $installer -ArgumentList "/quiet InstallAllUsers=1 PrependPath=1 Include_test=0" -Wait
Write-Host "PYTHON_DONE"
