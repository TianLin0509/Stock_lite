$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
$resp = Invoke-WebRequest 'https://www.python.org/ftp/python/3.13.2/python-3.13.2-amd64.exe' -Method Head -UseBasicParsing
$resp.Headers['Content-Length']
