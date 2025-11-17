# Tenable Export Suite v2 (TES)

[![Python](https://img.shields.io/badge/python-3.9%2B-blue.svg)]()
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)]()
[![Status](https://img.shields.io/badge/status-beta-orange.svg)]()

Advanced export tool for Tenable.io VM, WAS and Assets v2 into Excel, Parquet and DuckDB.

> This is the GitHub-friendly README. For full documentation, see [`README.md`](./README.md).

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
