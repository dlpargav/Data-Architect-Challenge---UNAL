"""
Docentes dataset processor — Silver layer transformation.

Responsibilities:
  - Rename raw SNIES column to the canonical metric name.
  - Filter to Bogotá institutions.
  - Detect and preserve semester-level granularity if present.
  - Run DQ checks.
  - Aggregate to (institution × year [× semester]).
"""

import logging

import pandas as pd

from src.processing.aggregation import filter_bogota, aggregate_by_institution_year
from src.processing.cleaning import (
    normalize_columns,
    validate_schema,
    drop_empty_rows,
    clean_institution_names,
)
from src.processing.data_quality import run_silver_dq
from src.utils.config import DOCENTES_REQUIRED_COLS, SEMESTER_COL_CANDIDATES

logger = logging.getLogger(__name__)

# Possible raw column names SNIES uses for the faculty count metric
_DOCENTES_COL_ALIASES = [
    "no._de_docentes",
    "no_de_docentes",
    "numero_de_docentes",
    "docentes",
    "total_docentes",
]


def _resolve_metric_col(df: pd.DataFrame) -> str:
    """Find whichever alias SNIES used this year."""
    for alias in _DOCENTES_COL_ALIASES:
        if alias in df.columns:
            return alias
    raise KeyError(
        f"Cannot find docentes metric column. Available: {list(df.columns)}"
    )


def process_docentes(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Full Silver transformation for the Docentes dataset.

    Args:
        df: Raw DataFrame from loader (columns already normalized).
        year: Source year — used for logging and DQ context.

    Returns:
        Aggregated Silver DataFrame.
    """
    df = drop_empty_rows(df)
    df = clean_institution_names(df)

    # ── Metric column resolution ───────────────────────────────────────────────
    raw_metric = _resolve_metric_col(df)
    if raw_metric != "numero_de_docentes":
        df = df.rename(columns={raw_metric: "numero_de_docentes"})
        logger.debug("Renamed '%s' → 'numero_de_docentes'", raw_metric)

    # ── Semester detection ─────────────────────────────────────────────────────
    semester_col = next(
        (c for c in SEMESTER_COL_CANDIDATES if c in df.columns), None
    )
    if semester_col:
        logger.info("Semester column detected ('%s') — preserving granularity.", semester_col)
        if semester_col != "semestre":
            df = df.rename(columns={semester_col: "semestre"})
    else:
        logger.info("No semester column found — annual aggregation only.")

    validate_schema(df, DOCENTES_REQUIRED_COLS, context=f"docentes_{year}")
    df = filter_bogota(df)

    # ── DQ checks ─────────────────────────────────────────────────────────────
    run_silver_dq(df, "docentes", "numero_de_docentes", year)

    return aggregate_by_institution_year(
        df, "numero_de_docentes", semester_col="semestre" if semester_col else None
    )