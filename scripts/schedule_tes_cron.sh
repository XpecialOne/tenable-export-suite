#!/usr/bin/env bash
# Example helper to install a daily cron job for TES v2.
# Adjust paths before running.

TES_DIR="/opt/tes"
PYTHON_BIN="$TES_DIR/.venv/bin/python"
LOG_DIR="$TES_DIR/logs"
EXPORT_DIR="$TES_DIR/exports"

mkdir -p "$LOG_DIR" "$EXPORT_DIR"

CRON_LINE="0 3 * * * cd $TES_DIR && $PYTHON_BIN Tenable_Export_Suite.py -o parquet duckdb --output-dir $EXPORT_DIR >> $LOG_DIR/tes_cron.log 2>&1"

(crontab -l 2>/dev/null | grep -v 'Tenable_Export_Suite.py'; echo "$CRON_LINE") | crontab -

echo "[TES] Cron job installed:"
echo "  $CRON_LINE"
