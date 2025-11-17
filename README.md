# Tenable Export Suite v2 (TES)

[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)]()
[![Status](https://img.shields.io/badge/status-beta-orange.svg)]()

Export **Tenable.io VM vulnerabilities, WAS findings and VM assets** into analytics-friendly formats (Excel, Parquet, DuckDB) in a single run.

This project contains a single script:

- `Tenable_Export_Suite.py` – the main entry point for **Tenable Export Suite v2 (TES)**

The script wraps the Tenable asynchronous export APIs, flattens the NDJSON output and writes clean tables ready for **Power BI**, **DuckDB**, or any data pipeline.

---

## Quick Start

```bash
git clone <your-repo-url>.git
cd tenable-export-suite-v2

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env               # fill with your Tenable API keys

python Tenable_Export_Suite.py -o excel parquet duckdb
```

For detailed usage, configuration, Power BI integration and examples, read:

- [`README.md`](./README.md)
- [`EXAMPLES.md`](./EXAMPLES.md)
- [`POWERBI_MODEL.md`](./POWERBI_MODEL.md)


## Table of contents

1. [Overview](#overview)  
2. [Features](#features)  
3. [Requirements](#requirements)  
4. [Installation](#installation)  
5. [Configuration (.env)](#configuration-env)  
6. [Usage](#usage)  
7. [What gets exported](#what-gets-exported)  
8. [Output formats](#output-formats)  
9. [Logging](#logging)  
10. [Troubleshooting](#troubleshooting)  
11. [Extending TES v2](#extending-tes-v2)  

---

## Overview

**Tenable Export Suite v2 (TES)** is a **one-shot export tool** for Tenable.io that:

- Starts asynchronous exports for:
  - **VM vulnerabilities** (`/vulns/export`)
  - **WAS findings** (`/was/v1/export/vulns`) – optional
  - **Assets v2** (`/assets/v2/export`)
- Polls the corresponding status endpoints until the export is finished
- Downloads each NDJSON **chunk**
- **Flattens nested JSON** structures into a tabular format
- Writes the result into:
  - A single **Excel** workbook (3 sheets)
  - Separate **Parquet** files
  - A **DuckDB** database (3 tables)

Everything is driven by environment variables (loaded via `.env`) so there is **no hard-coded secret** in the script.

TES v2 is an evolution of your initial export suite, with the same logic but clearer naming and improved documentation.

---

## Features

- ✅ **Three datasets in one run**
  - `VM_Vulnerabilities`
  - `WAS_Vulnerabilities` (can be disabled)
  - `Tenable_VM_Assets` (Assets v2 export)
- ✅ **Uses official Tenable export APIs**
  - VM: `POST /vulns/export`
  - WAS: `POST /was/v1/export/vulns`
  - Assets: `POST /assets/v2/export`
- ✅ **Robust status polling**
  - Polls `/.../status` every 5 seconds  
  - Times out after ~30 minutes by default
- ✅ **NDJSON handling**
  - Streams chunk responses
  - Parses each line safely
  - Flattens nested structures with a generic `flatten_dict` function
- ✅ **Data-frame friendly**
  - Nested dicts → flattened columns (`asset_id`, `asset_ipv4`, etc.)
  - Lists of dicts → JSON strings (so Parquet/Excel don’t break)
- ✅ **Multiple output formats**
  - Excel: one workbook, 3 sheets
  - Parquet: one file per dataset
  - DuckDB: one DB file, 3 tables
- ✅ **Excel-safe**
  - Truncates very long URLs and cell values to Excel limits
- ✅ **Parquet-safe**
  - Converts lists/dicts to JSON strings for Parquet compatibility
- ✅ **Configurable via env vars**
  - API keys, base URL, SSL verify, export size & chunk size, etc.
- ✅ **Detailed logging**
  - Per-run log file `tenable_export_<timestamp>.log` in the output directory

---

## Requirements

- **Python**: 3.9+ recommended
- **Python packages** (core):
  - `requests`
  - `pandas`
  - `python-dotenv`

- **Optional (but recommended) packages:**
  - For **Parquet** output:
    - `pyarrow`
  - For **Excel** output:
    - `xlsxwriter` (preferred; faster)
    - or `openpyxl` (fallback, via pandas)
  - For **DuckDB** output:
    - `duckdb`

Example installation:

```bash
pip install requests pandas python-dotenv pyarrow duckdb xlsxwriter openpyxl
```

If you don’t need a format, you can skip its dependency (e.g. no `pyarrow` if you don’t use Parquet).

---

## Installation

1. **Clone or copy** the project into a folder of your choice, e.g.:

   ```bash
   Tenable_Export_Suite.py
   README.md
   ```

2. (Optional but recommended) create and activate a virtual environment:

   ```bash
   python -m venv .venv
   source .venv/bin/activate           # Linux / macOS
   # .venv\Scripts\activate         # Windows
   ```

3. **Install dependencies**:

   ```bash
   pip install requests pandas python-dotenv pyarrow duckdb xlsxwriter openpyxl
   ```

4. Create a `.env` file next to the script (see next section).

---

## Configuration (.env)

TES v2 uses `python-dotenv`, so any variables defined in a local `.env` file will be loaded automatically.

Create `.env` in the same directory as the script:

```ini
# --- Required ---
TENABLE_ACCESS_KEY=your_access_key_here
TENABLE_SECRET_KEY=your_secret_key_here

# One of these (API URL / base URL)
# If both are set, TENABLE_API_URL takes precedence
TENABLE_API_URL=https://cloud.tenable.com
# or
# TENABLE_BASE_URL=https://cloud.tenable.com

# --- Optional security / networking ---
# Verify SSL certificates (recommended: true in production)
TENABLE_VERIFY_SSL=true   # set to false only if you really know what you're doing

# --- VM vulnerabilities export options ---
# How many assets to export per request in /vulns/export
TENABLE_VM_NUM_ASSETS=200
# Whether to include unlicensed assets
TENABLE_VM_INCLUDE_UNLICENSED=true

# --- WAS findings export options ---
# Number of assets for WAS exports
TENABLE_WAS_NUM_ASSETS=50
# Whether to include unlicensed WAS apps
TENABLE_WAS_INCLUDE_UNLICENSED=true

# --- Assets v2 export options ---
# Chunk size for /assets/v2/export (only valid for this endpoint)
TENABLE_ASSETS_CHUNK_SIZE=4000
```

### Notes on configuration

- **Base URL**
  - If neither `TENABLE_API_URL` nor `TENABLE_BASE_URL` is set, the script defaults to `https://cloud.tenable.com`.
- **SSL verification**
  - `TENABLE_VERIFY_SSL=false` will disable certificate verification.  
    Use only for lab/testing; **not recommended in production**.
- **Export size**
  - `TENABLE_VM_NUM_ASSETS`, `TENABLE_WAS_NUM_ASSETS` and `TENABLE_ASSETS_CHUNK_SIZE` control how many assets are exported in each run/chunk.
- The script internally sets filters:
  - `severity`: `["LOW", "MEDIUM", "HIGH", "CRITICAL"]` (informational is excluded)
  - VM `state`: `["OPEN", "REOPENED", "FIXED"]`
  - WAS `state`: `["OPEN", "REOPENED"]`

---

## Usage

From the directory containing the script and `.env`:

### Basic run (Excel + Parquet)

```bash
python Tenable_Export_Suite.py
```

This is equivalent to:

```bash
python Tenable_Export_Suite.py -o excel parquet
```

### Specify output formats

```bash
# Excel only
python Tenable_Export_Suite.py -o excel

# Parquet only
python Tenable_Export_Suite.py -o parquet

# DuckDB only
python Tenable_Export_Suite.py -o duckdb

# All three
python Tenable_Export_Suite.py -o excel parquet duckdb
```

### Change output directory

```bash
python Tenable_Export_Suite.py   -o excel parquet duckdb   --output-dir ./exports
```

The script will create the directory if it doesn’t exist.

### Disable WAS exports

If your API key doesn’t have WAS permissions, or you simply don’t need WAS findings, you can disable them:

```bash
python Tenable_Export_Suite.py --disable-was
```

> VM vulnerabilities and Assets v2 exports will still run as usual.

---

## What gets exported

TES v2 orchestrates three exports:

### 1. VM Vulnerabilities (`VM_Vulnerabilities`)

- Endpoint: `POST /vulns/export`
- Body structure (simplified):

  ```json
  {
    "num_assets": <TENABLE_VM_NUM_ASSETS>,
    "include_unlicensed": <TENABLE_VM_INCLUDE_UNLICENSED>,
    "filters": {
      "severity": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
      "state": ["OPEN", "REOPENED", "FIXED"]
    }
  }
  ```

- Status endpoint: `/vulns/export/{export_uuid}/status`
- Chunks endpoint: `/vulns/export/{export_uuid}/chunks/{chunk_id}`

Each chunk is NDJSON; every line is parsed and **flattened**:
- Nested keys like `asset.id` become `asset_id`
- Lists of dicts (e.g. plugin outputs) are stored as JSON strings

### 2. WAS Findings (`WAS_Vulnerabilities`)

- Endpoint: `POST /was/v1/export/vulns`
- Body structure (simplified):

  ```json
  {
    "num_assets": <TENABLE_WAS_NUM_ASSETS>,
    "include_unlicensed": <TENABLE_WAS_INCLUDE_UNLICENSED>,
    "filters": {
      "severity": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
      "state": ["OPEN", "REOPENED"]
    }
  }
  ```

- Status endpoint: `/was/v1/export/vulns/{export_uuid}/status`
- Chunks endpoint: `/was/v1/export/vulns/{export_uuid}/chunks/{chunk_id}`

If the API key doesn’t have WAS permissions (HTTP 403), TES logs a clear error and **skips WAS exports** without stopping the whole run.

### 3. Assets v2 (`Tenable_VM_Assets`)

- Endpoint: `POST /assets/v2/export`
- Body structure (simplified):

  ```json
  {
    "chunk_size": <TENABLE_ASSETS_CHUNK_SIZE>
    // "filters": { ... }    # none configured by default
  }
  ```

- Status endpoint: `/assets/export/{export_uuid}/status`
- Chunks endpoint: `/assets/export/{export_uuid}/chunks/{chunk_id}`

All assets are flattened similarly. For example:
- `network_interfaces` list → JSON string
- `tags` list → JSON string
- Top-level fields remain as simple columns

---

## Output formats

All outputs are timestamped using UTC: `YYYYMMDD_HHMMSS`.

### Excel

- File name:  
  `tenable_vm_was_assets_<timestamp>.xlsx`
- Sheets:
  - `VM_Vulnerabilities`
  - `WAS_Vulnerabilities`
  - `Tenable_VM_Assets`

If a dataset is empty, a sheet is still created with headers (if available) but no rows.

**Excel sanitization:**

- URL columns:
  - Truncated to **2079 characters** (Excel’s hyperlink limit)
- General cell content:
  - Truncated to **32767 characters** (Excel’s cell content limit)
- Lists/dicts:
  - Stored as JSON strings where needed, to avoid engine issues

### Parquet

- File names:
  - `VM_Vulnerabilities_<timestamp>.parquet`
  - `WAS_Vulnerabilities_<timestamp>.parquet`
  - `Tenable_VM_Assets_<timestamp>.parquet`

**Parquet sanitization:**

- Columns that contain lists/tuples/dicts are converted to JSON strings so that `pyarrow` can serialize them correctly.

### DuckDB

- File name:  
  `tenable_export_<timestamp>.duckdb`
- Tables:
  - `VM_Vulnerabilities`
  - `WAS_Vulnerabilities`
  - `Tenable_VM_Assets`

Empty DataFrames (no rows or no columns) are **skipped** so they don’t create unusable DuckDB tables.

Example usage with DuckDB CLI:

```sql
.open tenable_export_20250101_120000.duckdb
.tables
SELECT COUNT(*) FROM VM_Vulnerabilities;
SELECT severity, COUNT(*) FROM VM_Vulnerabilities GROUP BY 1;
```

---

## Logging

For each run, TES v2 creates a log file in the output directory:

```text
tenable_export_<timestamp>.log
```

It logs:

- Environment / setup info (output dir, log file path)
- Export start/stop messages for VM, WAS, Assets
- Polling status and chunks information
- Number of rows exported per dataset
- Column names and simple stats (e.g. asset types distribution)
- Warnings on:
  - Count mismatches (exported vs expected)
  - Decoding issues on NDJSON lines
  - Invalid chunk IDs / statuses
- Errors on:
  - HTTP failures (including response codes and truncated body)
  - Missing required environment variables
  - Export timeouts

Logs are written both to:
- The log file, and
- `stdout`

This makes it easy to follow progress in the terminal and still have a persistent log for later analysis.

---

## Troubleshooting

### 1. `Missing required environment variable: TENABLE_ACCESS_KEY`

You didn’t define `TENABLE_ACCESS_KEY` (or `TENABLE_SECRET_KEY`):

- Check your `.env` file
- Ensure you are running the script from the directory where `.env` is located
- Or export it directly in your shell:

  ```bash
  export TENABLE_ACCESS_KEY=...
  export TENABLE_SECRET_KEY=...
  ```

### 2. `403` when calling WAS export

Log example:

> `WAS findings export forbidden (403). This API key likely has no WAS access or insufficient permissions. Skipping WAS findings export.`

Meaning:
- Your Tenable API keys don’t have WAS permissions.
- TES v2 will **skip WAS** but still export VM vulnerabilities and assets.

Solution:
- Either request proper WAS permissions  
- Or keep using `--disable-was` if you don’t need WAS data

### 3. Export never finishes / timeout

Error:

> `Export status polling timed out after 360 attempts (30.0 minutes)`

Possible causes:

- Very large export size
- Tenable export backlog or issues
- Network timeouts

Actions:

- Reduce `TENABLE_VM_NUM_ASSETS` / `TENABLE_WAS_NUM_ASSETS` or `TENABLE_ASSETS_CHUNK_SIZE`
- Re-run the script
- If needed, increase the `max_retries` value for `poll_export_status` in the source code

### 4. Parquet output fails

If `pyarrow` is missing:

- TES logs:

  > `pyarrow is not installed; cannot write Parquet files. Install with: pip install pyarrow`

Install it:

```bash
pip install pyarrow
```

Or simply remove `parquet` from the `-o` outputs list.

### 5. DuckDB output is missing or empty

- If `duckdb` is not installed, TES logs a warning and skips DuckDB output.
- If a given DataFrame has **no rows or no columns**, it will be skipped as a table.  
  Check that your exports contain data (see row counts in the logs).

---

## Extending TES v2

TES v2 is structured in clear, reusable pieces (inside `Tenable_Export_Suite.py`):

- `build_session()`  
  Builds a Tenable session with headers and `base_url`.

- `poll_export_status()`  
  Generic export status polling (VM, WAS, assets).

- `ndjson_get()`  
  Downloads and flattens each NDJSON chunk.

- `flatten_dict()`  
  Recursively flattens JSON structures into flat dictionaries.

- `sanitize_for_excel()`, `sanitize_for_parquet()`  
  Take care of format-specific limitations.

- `export_vm_vulnerabilities()`, `export_was_findings()`, `export_assets_v2()`  
  Orchestrate the export + chunk download per dataset.

To add another export:

1. Implement a new `start_*_export()` using the appropriate Tenable endpoint.
2. Implement `export_*()` using `poll_export_status()` and `ndjson_get()`.
3. Add a DataFrame to the `df_map` in `main()`.
4. It will automatically be written to Excel/Parquet/DuckDB with the same naming convention.

---

Happy exporting with **Tenable Export Suite v2 (TES)**. 🚀
