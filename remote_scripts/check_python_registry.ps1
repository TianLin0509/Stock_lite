$ErrorActionPreference = "Stop"
Get-ItemProperty 'HKLM:\Software\Python\PythonCore\*\InstallPath' -ErrorAction SilentlyContinue |
    Select-Object PSChildName, ExecutablePath, @{Name='Path';Expression={$_.'(default)'}}
