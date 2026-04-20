"""
Column normalization and schema validation for SNIES raw data.

Cleaning contract:
  1. Normalize column names → snake_case, no accents, no special chars.
  2. Validate required columns exist (fail fast with a clear error).
  3. Standardize institution names (strip, title-case, collapse spaces).
  4. Drop fully empty rows.
"""

import logging
import unicodedata

import pandas as pd

logger = logging.getLogger(__name__)


class MissingColumnError(Exception):
    """Raised when an expected column is absent after normalization."""


# ─── Column name normalization ─────────────────────────────────────────────────

def _normalize_col_name(col: str) -> str:
    """Normalize a single column name to snake_case ASCII."""
    col = str(col).replace("\n", " ")
    col = " ".join(col.split())          # collapse whitespace
    col = col.lower()
    col = "".join(
        c for c in unicodedata.normalize("NFKD", col)
        if not unicodedata.combining(c)  # strip accent combining chars
    )
    col = col.replace(" ", "_")
    return col


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Apply column name normalization to the whole DataFrame."""
    df = df.copy()
    df.columns = df.columns.map(_normalize_col_name)
    return df


# ─── Schema validation ─────────────────────────────────────────────────────────

def validate_schema(df: pd.DataFrame, required_cols: set[str], context: str = "") -> None:
    """
    Raise MissingColumnError if any required columns are missing.
    Logs available columns to aid debugging across SNIES format changes.
    """
    missing = required_cols - set(df.columns)
    if missing:
        logger.error(
            "[%s] Missing columns: %s\nAvailable: %s",
            context, sorted(missing), sorted(df.columns),
        )
        raise MissingColumnError(
            f"[{context}] Required columns not found: {sorted(missing)}. "
            "Check config.py skiprows or update the column contract."
        )


# ─── Data hygiene ─────────────────────────────────────────────────────────────

def drop_empty_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows that are entirely null (common in SNIES Excel footers)."""
    before = len(df)
    df = df.dropna(how="all").reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d fully-empty rows.", dropped)
    return df


def standardize_institution_name(name: str) -> str:
    """
    Normalize an IES name for consistent display and fuzzy matching.
    - Strip surrounding whitespace
    - Collapse internal spaces
    - Title-case (handles ALL-CAPS SNIES names)
    """
    if not isinstance(name, str):
        return name
    name = " ".join(name.split())
    return name.strip().title()


def clean_institution_names(df: pd.DataFrame, col: str = "institucion_de_educacion_superior_(ies)") -> pd.DataFrame:
    """Apply institution name standardization if the column exists."""
    if col in df.columns:
        df = df.copy()
        df[col] = df[col].apply(standardize_institution_name)
    return df
