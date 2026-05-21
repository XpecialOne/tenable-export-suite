#!/usr/bin/env python3
"""
tenable_was_tagger.py
─────────────────────
Reads an Excel file (WAS App ID | Tag Value) and applies tags in the
format  "DHID : <value>"  to WAS applications in Tenable Vulnerability Management.

Key fix vs. previous version:
  - Column A is a WAS Application/Config ID, NOT a TVM asset UUID.
  - Each WAS App ID is resolved to its internal TVM Asset UUID via
    GET /was/v2/configs/{app_id} before any tag assignment is attempted.
  - Tag assignment still uses POST /tags/assets/assignments (bulk, 500/call).

Workflow
────────
1. Load credentials from environment / .env
2. Parse Excel → list of (was_app_id, tag_value) rows
3. Pre-load all existing Tenable tag categories + values into cache
   (paginated to handle large tenants)
4. Ensure category "DHID" and all required tag values exist
5. Resolve each WAS App ID → TVM Asset UUID
6. Bulk-assign tags (500 assets per call)
7. Print a detailed summary

Usage
─────
    export TENABLE_ACCESS_KEY="…"
    export TENABLE_SECRET_KEY="…"
    python tenable_was_tagger.py --file assets.xlsx [--dry-run] [--log-file tagger.log]
"""

import argparse
import logging
import os
import sys
import time
import uuid
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from tqdm import tqdm

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL        = "https://cloud.tenable.com"
TAG_CATEGORY    = "DHID"
BATCH_SIZE      = 500       # Tenable bulk tag assignment limit
PAGE_LIMIT      = 1000      # Pagination page size for tag listing
MAX_RETRIES     = 5
BACKOFF_BASE    = 2         # Exponential back-off base (seconds)


# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
def setup_logging(log_file: str | None) -> logging.Logger:
    logger = logging.getLogger("tenable_was_tagger")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        logger.addHandler(fh)

    return logger


# ──────────────────────────────────────────────────────────────────────────────
# Tenable API client
# ──────────────────────────────────────────────────────────────────────────────
class TenableClient:
    """
    Thin, stateful wrapper around the Tenable TVM REST API.

    Caches:
      categories_cache  { category_name  → category_uuid }
      tags_cache        { "category_name:tag_value" → tag_value_uuid }
    """

    def __init__(self, access_key: str, secret_key: str, logger: logging.Logger):
        self.log = logger
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-ApiKeys": f"accessKey={access_key}; secretKey={secret_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.categories_cache: dict[str, str] = {}   # name  → uuid
        self.tags_cache:       dict[str, str] = {}   # "Cat:Val" → uuid

    # ── Core HTTP wrapper ─────────────────────────────────────────────────────

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """
        Execute an HTTP request with:
          - Retry-After-aware 429 handling
          - Exponential back-off on transient errors
          - Structured error logging
        """
        url = f"{BASE_URL}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.request(method, url, timeout=30, **kwargs)

                if resp.status_code == 429:
                    # Respect Retry-After if present, else use exponential back-off
                    wait = int(resp.headers.get("Retry-After", BACKOFF_BASE ** attempt))
                    self.log.warning(
                        "Rate-limited (429). Waiting %ds before retry %d/%d…",
                        wait, attempt, MAX_RETRIES,
                    )
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return {} if resp.status_code == 204 or not resp.content else resp.json()

            except requests.exceptions.Timeout:
                self.log.warning(
                    "Timeout (attempt %d/%d): %s %s", attempt, MAX_RETRIES, method, url
                )
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(BACKOFF_BASE ** attempt)

            except requests.exceptions.HTTPError as exc:
                self.log.error(
                    "HTTP %s — %s %s\n  → %s",
                    exc.response.status_code, method, url,
                    exc.response.text[:400],
                )
                raise

        raise RuntimeError(f"Exceeded {MAX_RETRIES} retries for {method} {url}")

    # ── Tag category helpers ──────────────────────────────────────────────────

    def load_existing_tags(self) -> None:
        """
        Populate both caches from the live Tenable tenant.
        Uses pagination to guarantee completeness — avoids the silent
        truncation bug that occurs when a tenant has > PAGE_LIMIT values.
        """
        self.log.info("Loading existing tag categories…")
        data = self._request("GET", "/tags/categories")
        for cat in data.get("categories", []):
            self.categories_cache[cat["name"].strip()] = cat["uuid"]
        self.log.info("  → %d categor(ies) cached.", len(self.categories_cache))

        self.log.info("Loading existing tag values (paginated)…")
        offset = 0
        total_loaded = 0
        while True:
            data = self._request(
                "GET", f"/tags/values?limit={PAGE_LIMIT}&offset={offset}"
            )
            values = data.get("values", [])
            for v in values:
                key = f"{v.get('category_name', '').strip()}:{v.get('value', '').strip()}"
                self.tags_cache[key] = v["uuid"]
            total_loaded += len(values)
            if len(values) < PAGE_LIMIT:
                break
            offset += PAGE_LIMIT

        self.log.info("  → %d tag value(s) cached.", total_loaded)

    def get_or_create_category(self, name: str) -> str:
        """Return UUID of existing category, or create it and return its UUID."""
        if name in self.categories_cache:
            return self.categories_cache[name]

        self.log.info("Category '%s' not found — creating it.", name)
        resp = self._request(
            "POST", "/tags/categories",
            json={"name": name, "description": "Auto-created by tenable_was_tagger.py"},
        )
        # Tenable returns the new category at the top level of the response body
        cat_uuid = resp.get("uuid") or resp.get("category", {}).get("uuid")
        if not cat_uuid:
            raise ValueError(f"Unexpected response creating category '{name}': {resp}")
        self.categories_cache[name] = cat_uuid
        self.log.info("  → Created category '%s' (%s).", name, cat_uuid)
        return cat_uuid

    def get_or_create_tag_value(self, category_name: str, tag_value: str) -> str:
        """Return UUID of existing tag value, or create it and return its UUID."""
        cache_key = f"{category_name}:{tag_value}"
        if cache_key in self.tags_cache:
            return self.tags_cache[cache_key]

        # Ensure the parent category exists first
        self.get_or_create_category(category_name)

        self.log.info("Tag value '%s' not found — creating it.", cache_key)
        resp = self._request(
            "POST", "/tags/values",
            json={"category_name": category_name, "value": tag_value},
        )
        # Tenable returns the new tag value at the top level
        tv_uuid = resp.get("uuid") or resp.get("value", {}).get("uuid")
        if not tv_uuid:
            raise ValueError(f"Unexpected response creating tag value '{tag_value}': {resp}")
        self.tags_cache[cache_key] = tv_uuid
        self.log.info("  → Created tag value '%s' (%s).", cache_key, tv_uuid)
        return tv_uuid

    # ── WAS App ID → TVM Asset UUID resolution ───────────────────────────────

    def resolve_was_app_to_asset_uuid(self, app_id: str) -> str | None:
        """
        Resolve a WAS Application/Config ID to its linked TVM Asset UUID.

        WAS app IDs live under /was/v2/configs/{app_id}.
        The linked asset UUID is surfaced in the response — the exact field
        path may vary by tenant version; we try the two known locations.

        Returns None (and logs a warning) when:
          - The app ID is not found (404)
          - The app has never completed a scan (no asset record created yet)
        """
        try:
            resp = self._request("GET", f"/was/v2/configs/{app_id}")
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 404:
                self.log.error(
                    "WAS App ID '%s' not found (404). "
                    "Verify the ID exists in the WAS module.", app_id
                )
            else:
                self.log.error(
                    "Unexpected error fetching WAS app '%s': HTTP %s",
                    app_id, exc.response.status_code,
                )
            return None

        # Try the two known response shapes for the linked asset UUID
        asset_uuid = (
            resp.get("settings", {}).get("asset_uuid")
            or resp.get("asset_uuid")
        )

        if not asset_uuid:
            self.log.warning(
                "WAS App '%s' has no linked TVM asset UUID. "
                "This usually means the application has never completed a scan. "
                "Run at least one WAS scan against it first.", app_id
            )
            return None

        self.log.debug("Resolved WAS App '%s' → Asset UUID '%s'.", app_id, asset_uuid)
        return asset_uuid

    # ── Bulk tag assignment ───────────────────────────────────────────────────

    def bulk_assign_tags(
        self, asset_uuids: list[str], tag_value_uuid: str
    ) -> None:
        """
        POST /tags/assets/assignments
        Assigns a single tag value to a list of TVM asset UUIDs.
        Automatically chunks into BATCH_SIZE slices.

        The payload uses the object format the API requires:
          assets: [{"id": "…"}, …]
          tags:   [{"id": "…"}]
        """
        chunks = [
            asset_uuids[i: i + BATCH_SIZE]
            for i in range(0, len(asset_uuids), BATCH_SIZE)
        ]
        for idx, chunk in enumerate(chunks, start=1):
            self.log.info(
                "  Sending bulk assignment: %d asset(s), chunk %d/%d…",
                len(chunk), idx, len(chunks),
            )
            payload = {
                "action": "add",
                "assets": [{"id": uid} for uid in chunk],   # object format required
                "tags":   [{"id": tag_value_uuid}],          # object format required
            }
            self._request("POST", "/tags/assets/assignments", json=payload)
            self.log.info("    ✓ Chunk %d/%d assigned successfully.", idx, len(chunks))


# ──────────────────────────────────────────────────────────────────────────────
# Excel parsing
# ──────────────────────────────────────────────────────────────────────────────

def load_excel(path: str, logger: logging.Logger) -> list[tuple[str, str]]:
    """
    Parse the Excel file.
      Column A (index 0) → WAS Application ID
      Column B (index 1) → Tag Value

    Skips rows that are blank, contain 'nan', or whose App ID
    does not look like a UUID (logs a warning for each).
    """
    logger.info("Reading Excel file: %s", path)
    try:
        df = pd.read_excel(path, header=None, usecols=[0, 1], dtype=str)
    except Exception as exc:
        logger.critical("Cannot open Excel file: %s", exc)
        sys.exit(1)

    df.columns = ["app_id", "tag_value"]
    df["app_id"]    = df["app_id"].str.strip()
    df["tag_value"] = df["tag_value"].str.strip()

    before = len(df)
    df.dropna(subset=["app_id", "tag_value"], inplace=True)
    df = df[~df["app_id"].str.lower().eq("nan")]
    df = df[~df["tag_value"].str.lower().eq("nan")]
    dropped_blank = before - len(df)
    if dropped_blank:
        logger.warning("Dropped %d row(s) with blank App ID or Tag Value.", dropped_blank)

    # UUID format validation
    valid_rows: list[tuple[str, str]] = []
    invalid_count = 0
    for _, row in df.iterrows():
        try:
            uuid.UUID(row["app_id"])
            valid_rows.append((row["app_id"], row["tag_value"]))
        except ValueError:
            logger.warning(
                "Skipping row — App ID '%s' is not a valid UUID.", row["app_id"]
            )
            invalid_count += 1

    logger.info(
        "Loaded %d valid row(s). Skipped %d blank + %d non-UUID row(s).",
        len(valid_rows), dropped_blank, invalid_count,
    )
    return valid_rows


# ──────────────────────────────────────────────────────────────────────────────
# Core orchestration
# ──────────────────────────────────────────────────────────────────────────────

def run(
    rows: list[tuple[str, str]],
    client: TenableClient,
    logger: logging.Logger,
    dry_run: bool,
) -> None:
    total = len(rows)
    if total == 0:
        logger.warning("No valid rows to process.")
        return

    # ── Step 1: Pre-load existing tags ────────────────────────────────────────
    if not dry_run:
        client.load_existing_tags()

    # ── Step 2: Ensure all required tag values exist up-front ─────────────────
    unique_tag_values = {tv for _, tv in rows}
    logger.info(
        "Ensuring %d unique tag value(s) exist under category '%s'…",
        len(unique_tag_values), TAG_CATEGORY,
    )
    tag_value_uuids: dict[str, str] = {}   # tag_value → uuid

    if not dry_run:
        for tv in unique_tag_values:
            try:
                tag_value_uuids[tv] = client.get_or_create_tag_value(TAG_CATEGORY, tv)
            except Exception as exc:
                logger.error(
                    "Failed to resolve tag value '%s': %s — "
                    "rows using this value will be skipped.", tv, exc
                )

    # ── Step 3: Resolve WAS App IDs → TVM Asset UUIDs ─────────────────────────
    logger.info("Resolving %d WAS App ID(s) to TVM Asset UUIDs…", total)

    # Accumulate: { tag_value → [asset_uuid, …] }
    resolved: dict[str, list[str]] = {}
    failed_resolution: list[str] = []

    for app_id, tag_value in tqdm(rows, desc="Resolving WAS Apps", unit="app"):
        if dry_run:
            logger.info(
                "[DRY RUN] Would resolve WAS App '%s' and tag with '%s:%s'.",
                app_id, TAG_CATEGORY, tag_value,
            )
            continue

        if tag_value not in tag_value_uuids:
            logger.error(
                "Tag value '%s' was not resolved — skipping App ID '%s'.",
                tag_value, app_id,
            )
            failed_resolution.append(app_id)
            continue

        asset_uuid = client.resolve_was_app_to_asset_uuid(app_id)
        if not asset_uuid:
            failed_resolution.append(app_id)
            continue

        resolved.setdefault(tag_value, []).append(asset_uuid)

    if dry_run:
        logger.info("[DRY RUN] Complete — no changes made.")
        return

    # ── Step 4: Bulk assign in groups by tag value ────────────────────────────
    total_assigned = 0
    failed_assignment: list[str] = []

    for tag_value, asset_uuids in tqdm(
        resolved.items(), desc="Assigning tags", unit="tag"
    ):
        tv_uuid = tag_value_uuids[tag_value]
        logger.info(
            "Assigning tag '%s:%s' to %d asset(s)…",
            TAG_CATEGORY, tag_value, len(asset_uuids),
        )
        try:
            client.bulk_assign_tags(asset_uuids, tv_uuid)
            total_assigned += len(asset_uuids)
        except Exception as exc:
            logger.error(
                "Bulk assignment failed for tag '%s:%s': %s",
                TAG_CATEGORY, tag_value, exc,
            )
            failed_assignment.extend(asset_uuids)

    # ── Step 5: Summary ────────────────────────────────────────────────────────
    all_failed = failed_resolution + failed_assignment
    logger.info("─" * 60)
    logger.info("Run complete.")
    logger.info("  Total rows processed  : %d", total)
    logger.info("  Successfully tagged   : %d", total_assigned)
    logger.info("  Failed (resolution)   : %d", len(failed_resolution))
    logger.info("  Failed (assignment)   : %d", len(failed_assignment))

    if failed_resolution:
        logger.warning("WAS App IDs that could not be resolved:")
        for aid in failed_resolution:
            logger.warning("  - %s", aid)

    if failed_assignment:
        logger.warning("Asset UUIDs that failed tag assignment:")
        for aid in failed_assignment:
            logger.warning("  - %s", aid)

    if all_failed:
        logger.warning(
            "Tip: Apps with no linked asset UUID need at least one completed "
            "WAS scan before tags can be applied."
        )


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Apply DHID tags to Tenable WAS applications from an Excel file.\n"
            "Column A = WAS Application ID (UUID), Column B = Tag Value."
        )
    )
    p.add_argument("--file",     required=True,          help="Path to the .xlsx input file.")
    p.add_argument("--dry-run",  action="store_true",    help="Validate input only — make no API changes.")
    p.add_argument("--log-file", default="tenable_was_tagger.log", help="Log file path.")
    return p.parse_args()


def main() -> None:
    args   = parse_args()
    load_dotenv()
    logger = setup_logging(args.log_file)

    access_key = os.getenv("TENABLE_ACCESS_KEY", "").strip()
    secret_key = os.getenv("TENABLE_SECRET_KEY", "").strip()
    if not access_key or not secret_key:
        logger.critical(
            "TENABLE_ACCESS_KEY and TENABLE_SECRET_KEY must be set "
            "via environment variables or a .env file."
        )
        sys.exit(1)

    xlsx = Path(args.file)
    if not xlsx.is_file():
        logger.critical("Excel file not found: %s", xlsx)
        sys.exit(1)

    rows   = load_excel(str(xlsx), logger)
    client = TenableClient(access_key, secret_key, logger)

    if args.dry_run:
        logger.info("★  DRY RUN — no changes will be written to Tenable.")

    run(rows, client, logger, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
