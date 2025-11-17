# TES v2 – Power BI Integration Model

## Recommended Import Method

### Option 1 – Parquet (Best for performance)
Power BI → Get Data → Parquet → select:
- `VM_Vulnerabilities_YYYYMMDD.parquet`
- `WAS_Vulnerabilities_YYYYMMDD.parquet`
- `Tenable_VM_Assets_YYYYMMDD.parquet`

### Option 2 – DuckDB (Best for advanced analytics)
Install DuckDB Power BI connector.

Use queries:

```sql
SELECT * FROM VM_Vulnerabilities;
SELECT * FROM WAS_Vulnerabilities;
SELECT * FROM Tenable_VM_Assets;
```

## Recommended Relationships

- `VM_Vulnerabilities.asset_id` → `Tenable_VM_Assets.id`
- `WAS_Vulnerabilities.asset_uuid` → (if applicable) `Tenable_VM_Assets.uuid`

## Suggested Measures

- **Total Vulnerabilities:**  
  `COUNTROWS(VM_Vulnerabilities)`

- **Critical Vulns:**  
  `CALCULATE(COUNTROWS(VM_Vulnerabilities), VM_Vulnerabilities[severity] = "CRITICAL")`

- **Assets with Critical Vulns:**  
  `DISTINCTCOUNT(VM_Vulnerabilities[asset_id])`

## Suggested Dashboards

### 1. Vulnerability Overview
- Total vulnerabilities
- Per severity (bar chart)
- Trend over time (line chart)

### 2. Asset Security Posture
- Asset count by type
- Top 50 most vulnerable assets
- Coverage trends

### 3. WAS Findings Summary
- WAS vulnerabilities by severity
- Findings per application
- Exposure trends

### 4. Patch Management KPIs
- MTTR (Mean Time to Remediate)
- SLA compliance percentages
- Recurring vulnerabilities

## Dataset Refresh Strategy

- Automate TES v2 to run daily → write to a fixed output folder.
- Power BI scheduled refresh pulls latest parquet/duckdb.
- Use incremental refresh for large datasets.
