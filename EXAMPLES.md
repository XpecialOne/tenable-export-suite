# TES v2 – Examples

## Basic Export (Excel + Parquet)

```bash
python Tenable_Export_Suite.py
```

## Export All Formats

```bash
python Tenable_Export_Suite.py -o excel parquet duckdb
```

## Export to Custom Directory

```bash
python Tenable_Export_Suite.py -o excel parquet duckdb --output-dir ./exports
```

## Disable WAS Export

```bash
python Tenable_Export_Suite.py --disable-was
```

## Use Inside a Scheduled Task (crontab example)

```
0 3 * * * /usr/bin/python3 /opt/TES/Tenable_Export_Suite.py -o parquet --output-dir /opt/TES/exports
```

## Simple Power BI Import (Python)

```python
import duckdb
con = duckdb.connect("tenable_export_20250101_120000.duckdb")
df = con.execute("SELECT * FROM VM_Vulnerabilities").fetchdf()
```
