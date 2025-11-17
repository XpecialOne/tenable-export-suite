Param(
    [string]$TesDirectory = "C:\tes",
    [string]$TaskName = "Tenable Export Suite v2"
)

$pythonPath = Join-Path $TesDirectory ".venv\Scripts\python.exe"
$scriptPath = Join-Path $TesDirectory "Tenable_Export_Suite.py"
$exportDir = Join-Path $TesDirectory "exports"
$logDir = Join-Path $TesDirectory "logs"

New-Item -ItemType Directory -Force -Path $exportDir, $logDir | Out-Null

$action = New-ScheduledTaskAction -Execute $pythonPath -Argument ""$scriptPath" -o parquet duckdb --output-dir "$exportDir""
$trigger = New-ScheduledTaskTrigger -Daily -At 3:00am
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -RunLevel Highest -Force

Write-Host "[TES] Scheduled Task created: $TaskName" -ForegroundColor Green
