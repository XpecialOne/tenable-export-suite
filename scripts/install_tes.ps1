Param(
    [string]$PythonPath = "python"
)

Write-Host "[TES] Creating virtual environment..." -ForegroundColor Cyan
& $PythonPath -m venv .venv

Write-Host "[TES] Activating virtual environment..." -ForegroundColor Cyan
$venvActivate = ".\.venv\Scripts\Activate.ps1"
if (Test-Path $venvActivate) {
    & $venvActivate
} else {
    Write-Host "[TES] Could not find venv activation script at $venvActivate" -ForegroundColor Red
    exit 1
}

Write-Host "[TES] Installing requirements..." -ForegroundColor Cyan
pip install --upgrade pip
pip install -r requirements.txt

if (-Not (Test-Path ".env")) {
    Write-Host "[TES] Creating .env from .env.example..." -ForegroundColor Cyan
    Copy-Item ".env.example" ".env"
    Write-Host "[TES] Please edit .env and set your Tenable API keys." -ForegroundColor Yellow
}

Write-Host "[TES] Installation complete." -ForegroundColor Green
Write-Host "To run TES:" -ForegroundColor Green
Write-Host "  .\.venv\Scripts\Activate.ps1" -ForegroundColor Green
Write-Host "  python Tenable_Export_Suite.py -o excel parquet duckdb --output-dir .\exports" -ForegroundColor Green
