"""
File ingestion module — download from SNIES portal with local caching.

Design decisions:
- Cache-first: if the file already exists in data/raw/, skip the download.
  This makes the pipeline reproducible offline and avoids hitting the
  government server on every re-run.
- Format-agnostic: detects .xlsx vs .csv from the URL/filename extension.
- Adds 'ano' and 'ingestion_timestamp' metadata columns for traceability
  in the Bronze layer.
"""

import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import requests

from src.utils.config import RAW_DATA_DIR

logger = logging.getLogger(__name__)


class DownloadError(Exception):
    """Raised when a SNIES file cannot be fetched."""


def _local_path(local_name: str) -> Path:
    return RAW_DATA_DIR / local_name


def _download_file(url: str, dest: Path) -> None:
    """Stream-download a file from *url* to *dest*."""
    logger.info("Downloading %s -> %s", url, dest)
    try:
        with requests.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    fh.write(chunk)
    except requests.RequestException as exc:
        raise DownloadError(f"Failed to download {url}: {exc}") from exc
    logger.info("Downloaded %s (%.1f KB)", dest.name, dest.stat().st_size / 1024)


def _read_file(path: Path, skiprows: int, sheet_name) -> pd.DataFrame:
    """Read Excel or CSV into a DataFrame based on file extension."""
    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        return pd.read_excel(path, skiprows=skiprows, sheet_name=sheet_name)
    elif suffix == ".csv":
        # Try common Latin-American encodings
        for enc in ("utf-8", "latin-1", "iso-8859-1"):
            try:
                return pd.read_csv(path, skiprows=skiprows, encoding=enc)
            except UnicodeDecodeError:
                continue
        raise ValueError(f"Cannot decode CSV file {path} with known encodings.")
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def load_source_file(year: int, dataset_type: str, file_config: dict) -> pd.DataFrame:
    """
    Load a SNIES source file for a given year and dataset type.

    Args:
        year: Publication year (e.g., 2022).
        dataset_type: 'docentes' or 'estudiantes'.
        file_config: Dict with keys 'url', 'local_name', 'skiprows'.

    Returns:
        Raw DataFrame with added 'ano' and 'ingestion_timestamp' columns.
    """
    url: str = file_config["url"]
    local_name: str = file_config["local_name"]
    skiprows: int = file_config["skiprows"]
    sheet_name = file_config.get("sheet_name", 0)
    dest = _local_path(local_name)

    if dest.exists():
        logger.info("Cache hit — using local file: %s", dest)
    else:
        if "PLACEHOLDER" in url:
            raise DownloadError(
                f"URL for {dataset_type} {year} is still a placeholder. "
                "Please update SNIES_FILES in src/utils/config.py with the real URL."
            )
        _download_file(url, dest)

    df = _read_file(dest, skiprows, sheet_name)

    # ── Traceability metadata ──────────────────────────────────────────────────
    # SNIES files already contain an 'ano' column — only inject if absent
    has_ano = any(str(c).strip().lower() in ("ano", "año", "a\\xf1o", "ao") for c in df.columns)
    if not has_ano:
        df["ano"] = year
        logger.debug("Added 'ano' column (not present in source file).")
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    logger.info(
        "Loaded %s %s: %d rows × %d cols", dataset_type, year, len(df), len(df.columns)
    )
    return df
