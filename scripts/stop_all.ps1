# Stop all services

$projectPath = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)

Write-Host "Stopping all distributed clustering services..." -ForegroundColor Yellow

Get-CimInstance Win32_Process |
Where-Object {
    $_.CommandLine -like "*$projectPath*" -and
    ($_.Name -eq "python.exe" -or $_.Name -eq "powershell.exe")
} |
ForEach-Object {
    try {
        Stop-Process -Id $_.ProcessId -Force
        Write-Host "  ✓ Stopped process $($_.ProcessId)" -ForegroundColor Green
    } catch {
        Write-Host "  ✗ Failed to stop process $($_.ProcessId)" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "All services stopped!" -ForegroundColor Green
Write-Host ""