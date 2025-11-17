"""Tenable Export Suite v2 (TES)
Exports Tenable.io VM vulnerabilities, WAS findings, and Assets v2
to Excel, Parquet, and DuckDB.
Script name: Tenable_Export_Suite.py
"""

#!/usr/bin/env python3
import os
import sys
import argparse
import logging
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import requests
import pandas as pd
from dotenv import load_dotenv
load_dotenv()


try:
    import duckdb  # type: ignore
except ImportError:
    duckdb = None  # type: ignore

try:
    import xlsxwriter  # type: ignore
except ImportError:
    xlsxwriter = None  # type: ignore

try:
    import pyarrow  # type: ignore
except ImportError:
    pyarrow = None  # type: ignore


# ---------------------------------------------------------------------------
# Environment helpers
# ---------------------------------------------------------------------------

def get_env(name: str, default: Optional[str] = None, required: bool = False) -> str:
    val = os.getenv(name, default)
    if required and not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val or ""


def get_env_bool(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def get_env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if not val:
        return default
    try:
        return int(val)
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# HTTP / Tenable helpers
# ---------------------------------------------------------------------------

def build_session() -> requests.Session:
    access_key = get_env("TENABLE_ACCESS_KEY", required=True)
    secret_key = get_env("TENABLE_SECRET_KEY", required=True)
    # Support both TENABLE_API_URL and TENABLE_BASE_URL for compatibility
    base_url = get_env("TENABLE_API_URL") or get_env("TENABLE_BASE_URL", "https://cloud.tenable.com")
    base_url = base_url.rstrip("/")

    session = requests.Session()
    session.headers.update({
        "X-ApiKeys": f"accessKey={access_key}; secretKey={secret_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "tenable-export-suite/1.0",
    })
    session.verify = get_env_bool("TENABLE_VERIFY_SSL", True)
    # Attach base_url to session for convenience
    session.base_url = base_url  # type: ignore[attr-defined]
    return session


def flatten_dict(d: Dict[str, Any], parent_key: str = "", sep: str = "_") -> Dict[str, Any]:
    """
    Flatten a nested dictionary structure.
    Example: {"asset": {"id": 123, "name": "test"}} -> {"asset_id": 123, "asset_name": "test"}
    
    Handles:
    - Nested dictionaries (flattened with separator)
    - Lists of primitives (kept as-is)
    - Lists of dictionaries (JSON stringified for DataFrame compatibility)
    - Primitive values (kept as-is)
    """
    items: List[Tuple[str, Any]] = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            # Recursively flatten nested dictionaries
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if len(v) == 0:
                # Empty list
                items.append((new_key, v))
            elif isinstance(v[0], dict):
                # List of dictionaries - stringify for DataFrame compatibility
                items.append((new_key, json.dumps(v)))
            else:
                # List of primitives - keep as-is
                items.append((new_key, v))
        else:
            # Primitive value (str, int, float, bool, None)
            items.append((new_key, v))
    return dict(items)


def ndjson_get(session: requests.Session, url: str, log_prefix: str) -> List[Dict[str, Any]]:
    """
    Download a Tenable export chunk (NDJSON) and return list of dicts.
    Automatically flattens nested JSON structures for better DataFrame compatibility.
    """
    logging.info("%sDownloading chunk %s", log_prefix, url)
    rows: List[Dict[str, Any]] = []
    resp = session.get(url, stream=True, timeout=300)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        error_text = resp.text[:500] if resp.text else "No response body"
        logging.error("%sChunk download failed: %s %s", log_prefix, resp.status_code, error_text)
        raise e

    for line_num, line in enumerate(resp.iter_lines(), start=1):
        if not line:
            continue
        try:
            row = json.loads(line.decode("utf-8"))
            # Handle different JSON structures
            if isinstance(row, dict):
                # Flatten nested structures for better DataFrame handling
                flattened = flatten_dict(row)
                rows.append(flattened)
            elif isinstance(row, list):
                # If the row is a list, flatten each item if it's a dict
                for item in row:
                    if isinstance(item, dict):
                        rows.append(flatten_dict(item))
                    else:
                        # Non-dict items in list - create a simple dict wrapper
                        rows.append({"value": item})
            else:
                # Primitive value - wrap it in a dict
                rows.append({"value": row})
        except json.JSONDecodeError as e:
            logging.warning("%sFailed to decode line %d from %s: %s", log_prefix, line_num, url, str(e))
        except Exception as e:
            logging.warning("%sError processing line %d from %s: %s", log_prefix, line_num, url, str(e))
    return rows


def poll_export_status(session: requests.Session, status_url: str, max_retries: int = 360) -> Tuple[str, List[int], Optional[int]]:
    """
    Poll generic export status endpoint until FINISHED.
    Returns (status, chunks_available, total_count).
    
    Args:
        session: Requests session
        status_url: Status endpoint URL
        max_retries: Maximum number of polling attempts (default: 360 = 30 minutes at 5s intervals)
    """
    retries = 0
    while retries < max_retries:
        resp = session.get(status_url, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        status = data.get("status", "").upper()
        chunks = data.get("chunks_available") or []
        # Check for total count in response (some exports include this)
        total_count = data.get("total") or data.get("total_count") or data.get("count") or None
        logging.info("Status for %s: %s (attempt %d/%d)", status_url, status, retries + 1, max_retries)
        if total_count is not None:
            logging.info("Export reports total count: %d", total_count)
        if status in {"FINISHED", "ERROR", "CANCELLED"}:
            # Convert chunks to integers, handling both string and int types
            chunk_list = []
            for c in chunks:
                try:
                    chunk_list.append(int(c))
                except (ValueError, TypeError):
                    logging.warning("Invalid chunk ID: %s (type: %s), skipping", c, type(c).__name__)
            return status, chunk_list, total_count
        retries += 1
        time.sleep(5)
    
    # Timeout reached
    raise RuntimeError(f"Export status polling timed out after {max_retries} attempts ({(max_retries * 5) / 60:.1f} minutes)")


# ---------------------------------------------------------------------------
# VM Vulnerabilities export (/vulns/export)
# ---------------------------------------------------------------------------

def start_vm_export(session: requests.Session) -> str:
    """
    Start VM vulnerability export.

    Per Tenable docs (VM & WAS integrations):
      POST /vulns/export
      body:
        {
          "num_assets": 500,
          "include_unlicensed": true,
          "filters": {
            "since": 1234567890,
            "state": ["OPEN","REOPENED","FIXED"],
            "severity": ["LOW","MEDIUM","HIGH","CRITICAL"]
          }
        }
    """
    num_assets = get_env_int("TENABLE_VM_NUM_ASSETS", 200)
    include_unlicensed = get_env_bool("TENABLE_VM_INCLUDE_UNLICENSED", True)

    filters: Dict[str, Any] = {
        # Exclude informational by default
        "severity": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        # All states (open, reopened, fixed)
        "state": ["OPEN", "REOPENED", "FIXED"],
    }

    body: Dict[str, Any] = {
        "num_assets": num_assets,
        "include_unlicensed": include_unlicensed,
        "filters": filters,
    }

    url = f"{session.base_url}/vulns/export"  # type: ignore[attr-defined]
    logging.info("Starting VM vulnerabilities export with body=%s", body)
    resp = session.post(url, data=json.dumps(body), timeout=300)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.error("VM export start failed: status=%s, body=%s", resp.status_code, resp.text)
        raise e

    data = resp.json()
    export_uuid = data.get("export_uuid") or data.get("uuid")
    if not export_uuid:
        raise RuntimeError(f"VM export: no export_uuid in response: {data}")
    logging.info("VM export UUID: %s", export_uuid)
    return export_uuid


def export_vm_vulnerabilities(session: requests.Session) -> List[Dict[str, Any]]:
    export_uuid = start_vm_export(session)
    status_url = f"{session.base_url}/vulns/export/{export_uuid}/status"  # type: ignore[attr-defined]
    status, chunks, total_count = poll_export_status(session, status_url)
    if status != "FINISHED":
        logging.warning("VM export finished with status %s", status)
    logging.info("VM chunks available: %s", chunks)
    if total_count is not None:
        logging.info("Expected total vulnerabilities from export: %d", total_count)

    all_rows: List[Dict[str, Any]] = []
    if not chunks:
        logging.warning("VM export finished but no chunks available")
    else:
        for cid in chunks:
            chunk_url = f"{session.base_url}/vulns/export/{export_uuid}/chunks/{cid}"  # type: ignore[attr-defined]
            rows = ndjson_get(session, chunk_url, "[VM] ")
            all_rows.extend(rows)

    logging.info("VM vulnerabilities rows: %d", len(all_rows))
    if total_count is not None and len(all_rows) != total_count:
        logging.warning("Vulnerability count mismatch: expected %d, got %d", total_count, len(all_rows))
    return all_rows


# ---------------------------------------------------------------------------
# Assets export v2 (/assets/v2/export)  -> Tenable_VM_Assets
# ---------------------------------------------------------------------------

def start_assets_export_v2(session: requests.Session) -> str:
    """
    Start assets export v2.

    Per Tenable docs:
      POST /assets/v2/export
      body:
        {
          "chunk_size": 4000,
          "filters": { ... }
        }
    Only here is `chunk_size` valid (NOT on /vulns/export or WAS exports).
    """
    chunk_size = get_env_int("TENABLE_ASSETS_CHUNK_SIZE", 4000)
    
    # Note: Tenable Assets v2 export may have a default limit of 1000 assets per export
    # If you need more, you may need to use filters or make multiple export requests
    # Check the export status response for total_count to see if there are more assets

    # Include both VM hosts and WAS web applications
    # Remove the types filter to get all asset types, or explicitly include both
    filters: Dict[str, Any] = {
        "types": ["host", "webapp"],  # Include both VM hosts and WAS web applications
    }

    body: Dict[str, Any] = {
        "chunk_size": chunk_size,
        "filters": filters,
    }

    url = f"{session.base_url}/assets/v2/export"  # type: ignore[attr-defined]
    logging.info("Starting Assets v2 export with body=%s", body)
    resp = session.post(url, data=json.dumps(body), timeout=300)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.error("Assets v2 export start failed: status=%s, body=%s", resp.status_code, resp.text)
        raise e

    data = resp.json()
    export_uuid = data.get("export_uuid") or data.get("uuid")
    if not export_uuid:
        raise RuntimeError(f"Assets v2 export: no export_uuid in response: {data}")
    logging.info("Assets v2 export UUID: %s", export_uuid)
    return export_uuid


def export_assets_v2(session: requests.Session) -> List[Dict[str, Any]]:
    export_uuid = start_assets_export_v2(session)
    status_url = f"{session.base_url}/assets/export/{export_uuid}/status"  # type: ignore[attr-defined]
    status, chunks, total_count = poll_export_status(session, status_url)
    if status != "FINISHED":
        logging.warning("Assets v2 export finished with status %s", status)
    logging.info("Assets chunks available: %s", chunks)
    if total_count is not None:
        logging.info("Expected total assets from export: %d", total_count)

    all_rows: List[Dict[str, Any]] = []
    if not chunks:
        logging.warning("Assets v2 export finished but no chunks available")
    else:
        for cid in chunks:
            chunk_url = f"{session.base_url}/assets/export/{export_uuid}/chunks/{cid}"  # type: ignore[attr-defined]
            rows = ndjson_get(session, chunk_url, "[Assets] ")
            all_rows.extend(rows)
            logging.info("Chunk %d: fetched %d rows (total so far: %d)", cid, len(rows), len(all_rows))

    logging.info("Raw assets rows: %d", len(all_rows))
    if total_count is not None and len(all_rows) != total_count:
        logging.warning("Asset count mismatch: expected %d, got %d", total_count, len(all_rows))
    return all_rows


# ---------------------------------------------------------------------------
# WAS findings export (optional, /was/v1/export/vulns)
# ---------------------------------------------------------------------------

def start_was_export(session: requests.Session) -> Optional[str]:
    """
    Start WAS findings export.

    Per Tenable docs (VM & WAS integrations):
      POST /was/v1/export/vulns
      body:
        {
          "num_assets": 500,
          "include_unlicensed": true,
          "filters": {
            "since": 1234567890,
            "state": ["OPEN","REOPENED","FIXED"],
            "severity": ["LOW","MEDIUM","HIGH","CRITICAL"]
          }
        }
    """
    num_assets = get_env_int("TENABLE_WAS_NUM_ASSETS", 50)
    include_unlicensed = get_env_bool("TENABLE_WAS_INCLUDE_UNLICENSED", True)

    filters: Dict[str, Any] = {
        "severity": ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        "state": ["OPEN", "REOPENED"],
    }

    body: Dict[str, Any] = {
        "num_assets": num_assets,
        "include_unlicensed": include_unlicensed,
        "filters": filters,
    }

    url = f"{session.base_url}/was/v1/export/vulns"  # type: ignore[attr-defined]
    logging.info("Starting WAS findings export with body=%s", body)
    resp = session.post(url, data=json.dumps(body), timeout=300)
    if resp.status_code == 403:
        logging.error(
            "WAS findings export forbidden (403). This API key likely has no WAS access or insufficient permissions. "
            "Skipping WAS findings export."
        )
        return None
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logging.error("WAS findings export start failed: status=%s, body=%s", resp.status_code, resp.text)
        raise e

    data = resp.json()
    export_uuid = data.get("export_uuid") or data.get("uuid")
    if not export_uuid:
        raise RuntimeError(f"WAS findings export: no export_uuid in response: {data}")
    logging.info("WAS findings export UUID: %s", export_uuid)
    return export_uuid


def export_was_findings(session: requests.Session) -> List[Dict[str, Any]]:
    export_uuid = start_was_export(session)
    if not export_uuid:
        return []

    status_url = f"{session.base_url}/was/v1/export/vulns/{export_uuid}/status"  # type: ignore[attr-defined]
    status, chunks, total_count = poll_export_status(session, status_url)
    if status != "FINISHED":
        logging.warning("WAS findings export finished with status %s", status)
    logging.info("WAS chunks available: %s", chunks)
    if total_count is not None:
        logging.info("Expected total WAS findings from export: %d", total_count)

    all_rows: List[Dict[str, Any]] = []
    if not chunks:
        logging.warning("WAS findings export finished but no chunks available")
    else:
        for cid in chunks:
            chunk_url = f"{session.base_url}/was/v1/export/vulns/{export_uuid}/chunks/{cid}"  # type: ignore[attr-defined]
            rows = ndjson_get(session, chunk_url, "[WAS] ")
            all_rows.extend(rows)

    logging.info("WAS vulnerabilities rows: %d", len(all_rows))
    if total_count is not None and len(all_rows) != total_count:
        logging.warning("WAS findings count mismatch: expected %d, got %d", total_count, len(all_rows))
    return all_rows




# ---------------------------------------------------------------------------
# Output helpers (Parquet, Excel, DuckDB)
# ---------------------------------------------------------------------------

def sanitize_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame for Excel compatibility:
    - Truncate URLs to Excel's 2079 character limit
    - Truncate very long strings to 32767 characters (Excel cell limit)
    - Handle other Excel limitations
    """
    df_sanitized = df.copy()
    
    # Excel limits:
    # - URL length: 2079 characters
    # - Cell content: 32767 characters
    MAX_URL_LENGTH = 2079
    MAX_CELL_LENGTH = 32767
    
    for col in df_sanitized.columns:
        if df_sanitized[col].dtype == 'object':  # String/object columns
            def truncate_value(val):
                # Handle None and NaN values
                if val is None:
                    return val
                try:
                    if pd.isna(val):
                        return val
                except (ValueError, TypeError):
                    # pd.isna() can fail on lists/arrays, so we'll handle them separately
                    pass
                
                # Handle lists/arrays - convert to JSON string first
                if isinstance(val, (list, tuple)):
                    val_str = json.dumps(val) if val else ""
                else:
                    val_str = str(val)
                
                # Skip empty strings
                if not val_str:
                    return val
                
                # Check if it looks like a URL (only for string types, not JSON strings)
                if isinstance(val, str) and not isinstance(val, (list, tuple)):
                    if val_str.startswith('http://') or val_str.startswith('https://'):
                        if len(val_str) > MAX_URL_LENGTH:
                            logging.debug("Truncating URL in column %s from %d to %d characters", col, len(val_str), MAX_URL_LENGTH)
                            return val_str[:MAX_URL_LENGTH]
                
                # Truncate other long strings
                if len(val_str) > MAX_CELL_LENGTH:
                    logging.debug("Truncating value in column %s from %d to %d characters", col, len(val_str), MAX_CELL_LENGTH)
                    return val_str[:MAX_CELL_LENGTH]
                
                # Return original value if it's a list/tuple (will be converted to string by pandas)
                if isinstance(val, (list, tuple)):
                    return json.dumps(val) if len(json.dumps(val)) <= MAX_CELL_LENGTH else json.dumps(val)[:MAX_CELL_LENGTH]
                
                return val
            
            df_sanitized[col] = df_sanitized[col].apply(truncate_value)
    
    return df_sanitized


def sanitize_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sanitize DataFrame for Parquet compatibility:
    - Convert list/array columns to JSON strings (Parquet doesn't support lists directly)
    - Convert dict columns to JSON strings
    """
    df_sanitized = df.copy()
    
    for col in df_sanitized.columns:
        if df_sanitized[col].dtype == 'object':  # String/object columns
            def convert_value(val):
                # Handle None and NaN values safely
                if val is None:
                    return val
                try:
                    if pd.isna(val):
                        return val
                except (ValueError, TypeError):
                    # pd.isna() can fail on lists/arrays, so we'll handle them separately
                    pass
                
                # Convert lists, tuples, and dicts to JSON strings
                if isinstance(val, (list, tuple, dict)):
                    return json.dumps(val)
                return val
            
            df_sanitized[col] = df_sanitized[col].apply(convert_value)
    
    return df_sanitized


def write_parquet(df_map: Dict[str, pd.DataFrame], out_dir: str, ts: str) -> None:
    if pyarrow is None:
        logging.error("pyarrow is not installed; cannot write Parquet files. Install with: pip install pyarrow")
        raise RuntimeError("pyarrow is required for Parquet output but is not installed")
    
    for name, df in df_map.items():
        if df.empty:
            logging.info("Skipping empty Parquet file for %s", name)
            continue
        path = os.path.join(out_dir, f"{name}_{ts}.parquet")
        logging.info("Writing Parquet %s (%d rows)", path, len(df))
        try:
            # Sanitize data for Parquet compatibility (convert lists/dicts to JSON strings)
            df_sanitized = sanitize_for_parquet(df)
            df_sanitized.to_parquet(path, index=False, engine="pyarrow")
        except Exception as e:
            logging.error("Failed to write Parquet file %s: %s", path, str(e))
            raise


def write_excel(df_map: Dict[str, pd.DataFrame], out_dir: str, ts: str) -> str:
    xlsx_path = os.path.join(out_dir, f"tenable_vm_was_assets_{ts}.xlsx")
    logging.info("Writing Excel workbook %s", xlsx_path)
    
    # Use xlsxwriter if available (faster), otherwise fall back to openpyxl
    if xlsxwriter is not None:
        engine = "xlsxwriter"
    else:
        logging.warning("xlsxwriter not available, using openpyxl instead")
        engine = "openpyxl"
    
    with pd.ExcelWriter(xlsx_path, engine=engine) as writer:
        for sheet_name, df in df_map.items():
            if df.empty:
                logging.info("  Sheet %s (empty, creating with headers only)", sheet_name)
                df_sanitized = df
            else:
                logging.info("  Sheet %s (%d rows)", sheet_name, len(df))
                # Sanitize data for Excel compatibility (truncate long URLs/strings)
                df_sanitized = sanitize_for_excel(df)
            df_sanitized.to_excel(writer, sheet_name=sheet_name, index=False)
    
    return xlsx_path


def write_duckdb(df_map: Dict[str, pd.DataFrame], out_dir: str, ts: str, filename: Optional[str] = None) -> Optional[str]:
    if duckdb is None:
        logging.warning("duckdb is not installed; skipping DuckDB output.")
        return None

    db_name = filename or f"tenable_export_{ts}.duckdb"
    db_path = os.path.join(out_dir, db_name)
    logging.info("Writing DuckDB database %s", db_path)

    con = duckdb.connect(db_path)
    try:
        for table_name, df in df_map.items():
            # Avoid the "Need a DataFrame with at least one column" error
            if df.empty or df.shape[1] == 0:
                logging.info("  Table %s is empty (no columns or rows), skipping", table_name)
                continue
            logging.info("  Table %s (%d rows)", table_name, len(df))
            con.register("tmp_df", df)
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM tmp_df")
    finally:
        con.close()
    return db_path


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def setup_logging(out_dir: str, ts: str) -> None:
    os.makedirs(out_dir, exist_ok=True)
    log_file = os.path.join(out_dir, f"tenable_export_{ts}.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    logging.info("Log file: %s", log_file)


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tenable Export Suite v2 (TES): VM vulns, WAS vulns, and assets to Excel/Parquet/DuckDB"
    )
    parser.add_argument(
        "-o",
        "--outputs",
        nargs="+",
        choices=["excel", "parquet", "duckdb"],
        default=["excel", "parquet"],
        help="Output formats to generate (default: excel parquet).",
    )
    parser.add_argument(
        "--output-dir",
        default=".",
        help="Directory where files will be written (default: current directory).",
    )
    parser.add_argument(
        "--disable-was",
        action="store_true",
        help="Skip WAS vulnerabilities and WAS apps exports.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    setup_logging(args.output_dir, ts)
    logging.info("Starting Tenable export suite (run_ts=%s)", ts)

    session = build_session()

    # VM vulns
    logging.info("=== Exporting VM vulnerabilities ===")
    vm_rows = export_vm_vulnerabilities(session)

    # WAS vulns (optional)
    was_rows: List[Dict[str, Any]] = []
    if not args.disable_was:
        logging.info("=== Exporting WAS findings ===")
        was_rows = export_was_findings(session)
    else:
        logging.info("WAS exports disabled by --disable-was")

    # Assets v2 (VM assets)
    logging.info("=== Exporting Assets v2 (Tenable_VM_Assets) ===")
    assets_rows = export_assets_v2(session)

    # DataFrames - handle empty lists gracefully
    vm_df = pd.DataFrame(vm_rows) if vm_rows else pd.DataFrame()
    was_df = pd.DataFrame(was_rows) if was_rows else pd.DataFrame()
    assets_df = pd.DataFrame(assets_rows) if assets_rows else pd.DataFrame()
    
    # Log column information for debugging
    if not vm_df.empty:
        logging.info("VM_Vulnerabilities columns: %s", list(vm_df.columns))
    if not was_df.empty:
        logging.info("WAS_Vulnerabilities columns: %s", list(was_df.columns))
    if not assets_df.empty:
        logging.info("Tenable_VM_Assets columns: %s", list(assets_df.columns))
        logging.info("Tenable_VM_Assets row count: %d", len(assets_df))
        # Log asset type distribution if types column exists
        if "types" in assets_df.columns:
            # Handle case where types might be lists or strings
            try:
                # Convert lists to strings for counting
                types_series = assets_df["types"].apply(lambda x: str(x) if isinstance(x, (list, tuple)) else x)
                type_counts = types_series.value_counts()
                logging.info("Asset type distribution: %s", dict(type_counts))
            except Exception as e:
                logging.warning("Could not compute asset type distribution: %s", str(e))
                # Try to get unique types another way
                try:
                    unique_types = assets_df["types"].unique()
                    logging.info("Unique asset types found: %d", len(unique_types))
                except:
                    pass

    df_map: Dict[str, pd.DataFrame] = {
        "VM_Vulnerabilities": vm_df,
        "WAS_Vulnerabilities": was_df,
        "Tenable_VM_Assets": assets_df,
    }

    # Outputs
    if "parquet" in args.outputs:
        write_parquet(df_map, args.output_dir, ts)

    if "excel" in args.outputs:
        write_excel(df_map, args.output_dir, ts)

    if "duckdb" in args.outputs:
        write_duckdb(df_map, args.output_dir, ts)

    logging.info("Tenable export suite completed successfully.")


if __name__ == "__main__":
    main()
