#!/usr/bin/env python3
"""
tenable_was_tagger.py
─────────────────────
Reads an Excel file (Asset ID | Tag Value) and applies tags in the
format  "DHID : <value>"  to assets in Tenable Vulnerability Management.

Workflow
────────
1. Load credentials from environment / .env
2. Parse Excel → list of (asset_id, tag_value) rows
3. Ensure the tag category "DHID" and each required tag value exist in
   Tenable (create them on the fly if missing)
4. Batch-assign tags to assets using the  POST /tags/assets/assignments
   bulk endpoint (up to 500 per call)
5. Log every success / failure; retry on 429

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

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
BASE_URL = "https://cloud.tenable.com"
TAG_CATEGORY_NAME = "DHID"
BATCH_SIZE = 500          # Tenable allows up to 500 assets per bulk assignment
MAX_RETRIES = 5
BACKOFF_BASE = 2          # seconds  (doubles on every retry)

# ──────────────────────────────────────────────────────────────────────────────
# Logging setup  (console + optional file)
# ──────────────────────────────────────────────────────────────────────────────
def setup_logging(log_file: str | None) -> logging.Logger:
    logger = logging.getLogger("tenable_tagger")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

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
    """Thin wrapper around the Tenable TVM REST API."""

    def __init__(self, access_key: str, secret_key: str, logger: logging.Logger):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-ApiKeys": f"accessKey={access_key}; secretKey={secret_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        self.log = logger

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Execute an HTTP request with retry/back-off on 429."""
        url = f"{BASE_URL}{path}"
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.request(method, url, timeout=30, **kwargs)

                if resp.status_code == 429:
                    wait = BACKOFF_BASE ** attempt
                    self.log.warning("Rate-limited (429). Waiting %ds before retry %d/%d…", wait, attempt, MAX_RETRIES)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()

                # 204 No Content → return empty dict
                if resp.status_code == 204 or not resp.content:
                    return {}

                return resp.json()

            except requests.exceptions.Timeout:
                self.log.warning("Request timed out (attempt %d/%d): %s %s", attempt, MAX_RETRIES, method, url)
                if attempt == MAX_RETRIES:
                    raise
                time.sleep(BACKOFF_BASE ** attempt)

            except requests.exceptions.HTTPError as exc:
                self.log.error("HTTP %s for %s %s — %s", exc.response.status_code, method, url, exc.response.text[:300])
                raise

        raise RuntimeError(f"Exceeded {MAX_RETRIES} retries for {method} {url}")

    # ── Tag category & value management ──────────────────────────────────────

    def list_tag_categories(self) -> list[dict]:
        """Return all tag categories."""
        data = self._request("GET", "/tags/categories")
        return data.get("categories", [])

    def create_tag_category(self, name: str) -> dict:
        """Create a tag category and return it."""
        return self._request("POST", "/tags/categories", json={"name": name})

    def get_or_create_category(self, name: str) -> dict:
        """Return existing category or create it."""
        for cat in self.list_tag_categories():
            if cat["name"].strip().lower() == name.strip().lower():
                self.log.debug("Found existing tag category '%s' (uuid=%s)", name, cat["uuid"])
                return cat
        self.log.info("Tag category '%s' not found — creating it.", name)
        cat = self.create_tag_category(name)
        self.log.info("Created tag category '%s' (uuid=%s).", name, cat["uuid"])
        return cat

    def list_tag_values(self, category_uuid: str) -> list[dict]:
        """Return all tag values for a given category."""
        data = self._request("GET", f"/tags/values?f=category_uuid:eq:{category_uuid}&limit=10000")
        return data.get("values", [])

    def create_tag_value(self, category_uuid: str, value: str) -> dict:
        """Create a tag value under a category and return it."""
        return self._request(
            "POST",
            "/tags/values",
            json={"category_uuid": category_uuid, "value": value},
        )

    def get_or_create_tag_value(self, category_uuid: str, value: str, value_cache: dict) -> str:
        """
        Return the uuid of the tag value (creating it if needed).
        `value_cache` is a mutable dict used as an in-memory cache so we
        don't hammer the API with repeated lookups for the same value.
        """
        cache_key = f"{category_uuid}::{value}"
        if cache_key in value_cache:
            return value_cache[cache_key]

        for tv in self.list_tag_values(category_uuid):
            if tv["value"].strip().lower() == value.strip().lower():
                value_cache[cache_key] = tv["uuid"]
                self.log.debug("Found existing tag value '%s' (uuid=%s)", value, tv["uuid"])
                return tv["uuid"]

        self.log.info("Tag value '%s' not found — creating it.", value)
        tv = self.create_tag_value(category_uuid, value)
        value_cache[cache_key] = tv["uuid"]
        self.log.info("Created tag value '%s' (uuid=%s).", value, tv["uuid"])
        return tv["uuid"]

    # ── Asset lookup ──────────────────────────────────────────────────────────

    def asset_exists(self, asset_id: str) -> bool:
        """Quick existence check; returns False if 404."""
        try:
            self._request("GET", f"/assets/{asset_id}")
            return True
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 404:
                return False
            raise

    # ── Bulk tag assignment ───────────────────────────────────────────────────

    def bulk_assign_tags(self, asset_ids: list[str], tag_value_uuids: list[str]) -> dict:
        """
        POST /tags/assets/assignments
        Assigns one or more tag values to one or more assets in a single call.
        Tenable supports up to 500 assets per request.
        """
        payload = {
            "action": "add",
            "assets": [{"id": aid} for aid in asset_ids],
            "tags": [{"id": tvid} for tvid in tag_value_uuids],
        }
        return self._request("POST", "/tags/assets/assignments", json=payload)


# ──────────────────────────────────────────────────────────────────────────────
# Excel parsing
# ──────────────────────────────────────────────────────────────────────────────

def load_excel(path: str, logger: logging.Logger) -> list[tuple[str, str]]:
    """
    Parse the Excel file.
    Column A → asset_id  (string, stripped)
    Column B → tag_value (string, stripped)
    Rows with missing/blank values are skipped with a warning.
    """
    logger.info("Loading Excel file: %s", path)
    try:
        df = pd.read_excel(path, header=None, dtype=str)
    except Exception as exc:
        logger.critical("Failed to open Excel file: %s", exc)
        sys.exit(1)

    if df.shape[1] < 2:
        logger.critical("Excel must have at least 2 columns (Asset ID, Tag Value).")
        sys.exit(1)

    rows: list[tuple[str, str]] = []
    for idx, row in df.iterrows():
        asset_id = str(row.iloc[0]).strip() if pd.notna(row.iloc[0]) else ""
        tag_value = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""

        if not asset_id or not tag_value or asset_id.lower() == "nan":
            logger.warning("Row %d: blank asset_id or tag_value — skipped.", idx + 1)
            continue

        # Basic UUID format check (Tenable asset IDs are UUIDs)
        try:
            uuid.UUID(asset_id)
        except ValueError:
            logger.warning("Row %d: '%s' does not look like a UUID — skipped.", idx + 1, asset_id)
            continue

        rows.append((asset_id, tag_value))

    logger.info("Loaded %d valid rows from Excel.", len(rows))
    return rows


# ──────────────────────────────────────────────────────────────────────────────
# Core tagging logic
# ──────────────────────────────────────────────────────────────────────────────

def run_tagging(
    rows: list[tuple[str, str]],
    client: TenableClient,
    logger: logging.Logger,
    dry_run: bool,
) -> None:
    total = len(rows)
    if total == 0:
        logger.warning("No rows to process. Exiting.")
        return

    # ── Step 1: Resolve tag category ─────────────────────────────────────────
    logger.info("Resolving tag category '%s'…", TAG_CATEGORY_NAME)
    if dry_run:
        logger.info("[DRY RUN] Would look up / create category '%s'.", TAG_CATEGORY_NAME)
        category_uuid = "dry-run-category-uuid"
    else:
        category = client.get_or_create_category(TAG_CATEGORY_NAME)
        category_uuid = category["uuid"]

    # ── Step 2: Resolve all unique tag values up-front ────────────────────────
    unique_values = {tag_value for _, tag_value in rows}
    logger.info("Resolving %d unique tag value(s) under category '%s'…", len(unique_values), TAG_CATEGORY_NAME)
    value_cache: dict[str, str] = {}  # "category_uuid::value" → tag_value_uuid

    if not dry_run:
        for val in unique_values:
            try:
                client.get_or_create_tag_value(category_uuid, val, value_cache)
            except Exception as exc:
                logger.error("Failed to resolve tag value '%s': %s — rows using this value will be skipped.", val, exc)

    # ── Step 3: Validate assets & build batches grouped by tag value ──────────
    # Structure: { tag_value → [asset_id, …] }
    batches: dict[str, list[str]] = {}
    failed_assets: list[str] = []

    for i, (asset_id, tag_value) in enumerate(rows, start=1):
        if i % 10 == 0 or i == total:
            logger.info("Validating asset %d / %d  (%s)…", i, total, asset_id)

        if dry_run:
            logger.info("[DRY RUN] Would validate and tag asset '%s' with '%s:%s'.",
                        asset_id, TAG_CATEGORY_NAME, tag_value)
            continue

        # Verify asset exists
        try:
            if not client.asset_exists(asset_id):
                logger.error("Asset '%s' not found in Tenable — skipping.", asset_id)
                failed_assets.append(asset_id)
                continue
        except Exception as exc:
            logger.error("Error checking asset '%s': %s — skipping.", asset_id, exc)
            failed_assets.append(asset_id)
            continue

        # Check tag value was resolved
        cache_key = f"{category_uuid}::{tag_value}"
        if cache_key not in value_cache:
            logger.error("Tag value '%s' was not resolved — skipping asset '%s'.", tag_value, asset_id)
            failed_assets.append(asset_id)
            continue

        batches.setdefault(tag_value, []).append(asset_id)

    if dry_run:
        logger.info("[DRY RUN] Complete. No changes were made.")
        return

    # ── Step 4: Bulk assign in batches of BATCH_SIZE ──────────────────────────
    total_assigned = 0
    total_errors = 0

    for tag_value, asset_ids in batches.items():
        cache_key = f"{category_uuid}::{tag_value}"
        tag_value_uuid = value_cache[cache_key]

        # Chunk into BATCH_SIZE slices
        chunks = [asset_ids[i: i + BATCH_SIZE] for i in range(0, len(asset_ids), BATCH_SIZE)]
        for chunk_idx, chunk in enumerate(chunks, start=1):
            logger.info(
                "Assigning tag '%s:%s' to %d asset(s) [chunk %d/%d]…",
                TAG_CATEGORY_NAME, tag_value, len(chunk), chunk_idx, len(chunks),
            )
            try:
                client.bulk_assign_tags(chunk, [tag_value_uuid])
                total_assigned += len(chunk)
                logger.info("  ✓ Successfully tagged %d asset(s).", len(chunk))
            except Exception as exc:
                logger.error("  ✗ Bulk assignment failed for tag '%s': %s", tag_value, exc)
                total_errors += len(chunk)
                failed_assets.extend(chunk)

    # ── Step 5: Summary ────────────────────────────────────────────────────────
    logger.info("─" * 60)
    logger.info("Tagging complete.")
    logger.info("  Total rows in file  : %d", total)
    logger.info("  Successfully tagged : %d", total_assigned)
    logger.info("  Failed / skipped    : %d", len(failed_assets))

    if failed_assets:
        logger.warning("Failed asset IDs:")
        for aid in failed_assets:
            logger.warning("  - %s", aid)


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply DHID tags to Tenable TVM assets from an Excel file."
    )
    parser.add_argument("--file", required=True, help="Path to the .xlsx input file.")
    parser.add_argument("--dry-run", action="store_true", help="Parse & validate only; make no API changes.")
    parser.add_argument("--log-file", default="tenable_tagger.log", help="Path to the log file (default: tenable_tagger.log).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Load .env if present (does not override real env vars)
    load_dotenv()

    logger = setup_logging(args.log_file)

    # ── Credentials ───────────────────────────────────────────────────────────
    access_key = os.getenv("TENABLE_ACCESS_KEY", "").strip()
    secret_key = os.getenv("TENABLE_SECRET_KEY", "").strip()

    if not access_key or not secret_key:
        logger.critical(
            "TENABLE_ACCESS_KEY and TENABLE_SECRET_KEY must be set "
            "as environment variables or in a .env file."
        )
        sys.exit(1)

    # ── Input file ────────────────────────────────────────────────────────────
    xlsx_path = Path(args.file)
    if not xlsx_path.is_file():
        logger.critical("Excel file not found: %s", xlsx_path)
        sys.exit(1)

    rows = load_excel(str(xlsx_path), logger)

    # ── Run ───────────────────────────────────────────────────────────────────
    client = TenableClient(access_key, secret_key, logger)

    if args.dry_run:
        logger.info("★ DRY RUN mode — no changes will be made to Tenable.")

    run_tagging(rows, client, logger, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
