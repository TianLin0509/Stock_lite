$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
try {
    $resp = Invoke-WebRequest 'https://github.com/git-for-windows/git/releases/download/v2.49.0.windows.1/Git-2.49.0-64-bit.exe' -Method Head -UseBasicParsing
    $resp.Headers['Content-Length']
} catch {
    $_.Exception.Message
}
