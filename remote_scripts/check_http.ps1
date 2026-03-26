$ErrorActionPreference = "Continue"

Write-Host "=== PROCESS 3096/PORT 80 ==="
Get-NetTCPConnection -LocalPort 80 -ErrorAction SilentlyContinue |
    Select-Object LocalAddress, LocalPort, State, OwningProcess

Write-Host "=== HTTP /report/test ==="
try {
    $resp = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1/report/test" -TimeoutSec 10
    Write-Host $resp.StatusCode
    Write-Host $resp.Content
} catch {
    if ($_.Exception.Response) {
        Write-Host ([int]$_.Exception.Response.StatusCode)
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            Write-Host ($reader.ReadToEnd())
        } catch {
        }
    } else {
        Write-Host $_.Exception.Message
    }
}

Write-Host "=== HTTP /wechat ==="
try {
    $resp = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1/wechat" -TimeoutSec 10
    Write-Host $resp.StatusCode
    Write-Host $resp.Content
} catch {
    if ($_.Exception.Response) {
        Write-Host ([int]$_.Exception.Response.StatusCode)
        try {
            $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
            Write-Host ($reader.ReadToEnd())
        } catch {
        }
    } else {
        Write-Host $_.Exception.Message
    }
}
