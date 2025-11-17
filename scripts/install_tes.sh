#!/usr/bin/env bash
set -e

echo "[TES] Creating virtual environment..."
python -m venv .venv

echo "[TES] Activating virtual environment..."
source .venv/bin/activate

echo "[TES] Installing requirements..."
pip install --upgrade pip
pip install -r requirements.txt

if [ ! -f ".env" ]; then
  echo "[TES] Creating .env from .env.example..."
  cp .env.example .env
  echo "[TES] Please edit .env and set your Tenable API keys."
fi

echo "[TES] Installation complete. To run:"
echo "  source .venv/bin/activate"
echo "  python Tenable_Export_Suite.py -o excel parquet duckdb --output-dir ./exports"
