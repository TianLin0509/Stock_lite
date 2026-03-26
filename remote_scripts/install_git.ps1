$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$installer = Join-Path $env:TEMP "Git-2.49.0-64-bit.exe"
Invoke-WebRequest "https://github.com/git-for-windows/git/releases/download/v2.49.0.windows.1/Git-2.49.0-64-bit.exe" -OutFile $installer
Start-Process -FilePath $installer -ArgumentList "/VERYSILENT /NORESTART /NOCANCEL" -Wait
Write-Host "GIT_DONE"
