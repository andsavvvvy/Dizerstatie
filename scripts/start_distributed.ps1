# Distributed Clustering System - Startup Script
# Starts all nodes + orchestrator + UI in separate windows

$projectPath = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$venvActivate = "$projectPath\.venv\Scripts\Activate.ps1"

Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "        DISTRIBUTED CLUSTERING SYSTEM                            " -ForegroundColor Cyan
Write-Host "        Starting All Services...                                 " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ""

function Start-Service {
    param (
        [string]$Name,
        [string]$Script,
        [int]$Port,
        [string]$Type,
        [string]$WindowStyle = "Minimized"
    )
    
    $title = "[$Type] $Name - Port $Port"
    
    Start-Process powershell `
        -WindowStyle $WindowStyle `
        -ArgumentList "-NoExit", "-Command", @"
cd '$projectPath'
`$host.ui.RawUI.WindowTitle='$title'
`$host.ui.RawUI.BackgroundColor = 'Black'
`$env:PYTHONPATH='$projectPath'
. '$venvActivate'
Write-Host '=========================================' -ForegroundColor Green
Write-Host ' SERVICE: $Name' -ForegroundColor Green
Write-Host ' TYPE: $Type' -ForegroundColor Green
Write-Host ' PORT: $Port' -ForegroundColor Green
Write-Host '=========================================' -ForegroundColor Green
Write-Host ''
python $Script
"@
    
    Write-Host "  [OK] Started: $Name (Port $Port)" -ForegroundColor Green
}

# Start Global Orchestrator FIRST
Write-Host "Starting Global Orchestrator..." -ForegroundColor Yellow
Start-Service `
    -Name "Global Orchestrator" `
    -Script "orchestrator_global\app.py" `
    -Port 7000 `
    -Type "COORDINATOR" `
    -WindowStyle "Minimized"

Start-Sleep -Seconds 3

# Start Distributed Nodes
Write-Host ""
Write-Host "Starting Distributed Nodes..." -ForegroundColor Yellow

Start-Service `
    -Name "Medical Node" `
    -Script "nodes\node_medical\local_miner.py" `
    -Port 6001 `
    -Type "HEALTHCARE"

Start-Sleep -Seconds 1

Start-Service `
    -Name "Retail Node" `
    -Script "nodes\node_retail\local_miner.py" `
    -Port 6002 `
    -Type "RETAIL"

Start-Sleep -Seconds 1

Start-Service `
    -Name "IoT Node" `
    -Script "nodes\node_iot\local_miner.py" `
    -Port 6003 `
    -Type "IOT"

Start-Sleep -Seconds 2

# Start Web UI
Write-Host ""
Write-Host "Starting Web Interface..." -ForegroundColor Yellow

Start-Service `
    -Name "Web UI" `
    -Script "ui\app.py" `
    -Port 9000 `
    -Type "WEB_INTERFACE" `
    -WindowStyle "Normal"

Start-Sleep -Seconds 2

# Summary
Write-Host ""
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host "        ALL SERVICES STARTED SUCCESSFULLY!                       " -ForegroundColor Cyan
Write-Host "=================================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "System Endpoints:" -ForegroundColor Yellow
Write-Host "  Web Dashboard:       http://localhost:9000" -ForegroundColor White -BackgroundColor DarkBlue
Write-Host "  Global Orchestrator: http://localhost:7000/health" -ForegroundColor White
Write-Host "  Medical Node:        http://localhost:6001/health" -ForegroundColor White
Write-Host "  Retail Node:         http://localhost:6002/health" -ForegroundColor White
Write-Host "  IoT Node:            http://localhost:6003/health" -ForegroundColor White
Write-Host ""
Write-Host "Quick Actions:" -ForegroundColor Yellow
Write-Host "  Open Web UI:         Start-Process 'http://localhost:9000'" -ForegroundColor White
Write-Host "  Run CLI Analysis:    python scripts\run_full_analysis.py" -ForegroundColor White
Write-Host "  Check Status:        python scripts\check_status.py" -ForegroundColor White
Write-Host "  Stop All Services:   .\scripts\stop_all.ps1" -ForegroundColor White
Write-Host ""

# Auto-open browser after 3 seconds
Write-Host "Opening web browser in 3 seconds..." -ForegroundColor Yellow
Start-Sleep -Seconds 3
Start-Process "http://localhost:9000"
