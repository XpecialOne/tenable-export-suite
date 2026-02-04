Param(
    # Folder where Tenable_Export_Suite.py and its .venv live
    [string]$TesDirectory = "C:\tes",

    # Task name in Windows Task Scheduler
    [string]$TaskName = "Tenable Export Suite v2",

    # SharePoint/OneDrive-synced folder where the Excel output should land
    # Example:
    #   "C:\Users\<you>\OneDrive - <Org>\Shared Documents\TenableExports"
    [string]$ExportDir,

    # Daily run time (24h format)
    [string]$RunAt = "08:00"
)

$pythonPath = Join-Path $TesDirectory ".venv\Scripts\python.exe"
$scriptPath = Join-Path $TesDirectory "Tenable_Export_Suite.py"
$logDir = Join-Path $TesDirectory "logs"

if (-not $ExportDir -or $ExportDir.Trim() -eq "") {
    $ExportDir = Join-Path $TesDirectory "exports"
    Write-Host "[TES] ExportDir not provided. Using: $ExportDir" -ForegroundColor Yellow
}
$exportDir = $ExportDir

New-Item -ItemType Directory -Force -Path $exportDir, $logDir | Out-Null

# Excel only, output to SharePoint folder, logs to local logs folder (same dir as script)
$arguments = "`"$scriptPath`" -o excel --output-dir `"$exportDir`" --log-dir `"$logDir`""
$action   = New-ScheduledTaskAction -Execute $pythonPath -Argument $arguments -WorkingDirectory $TesDirectory
$trigger  = New-ScheduledTaskTrigger -Daily -At ([datetime]::ParseExact($RunAt,"HH:mm",$null))
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries

# Run as current user (recommended when ExportDir is an OneDrive/SharePoint-synced path)
$principal = New-ScheduledTaskPrincipal -UserId $env:USERNAME -LogonType S4U -RunLevel Highest

Register-ScheduledTask -TaskName $TaskName -Action $action -Trigger $trigger -Settings $settings -Principal $principal -Force | Out-Null

Write-Host "[TES] Scheduled Task created/updated: $TaskName (Daily at $RunAt)" -ForegroundColor Green
Write-Host "[TES] ExportDir: $exportDir" -ForegroundColor Green
Write-Host "[TES] LogDir:    $logDir" -ForegroundColor Green
