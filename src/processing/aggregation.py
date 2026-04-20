"""
Aggregation helpers — Bogotá filter and groupby aggregation.

Updated to support optional semester granularity.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

IES_COL = "institucion_de_educacion_superior_(ies)"
MUNICIPALITY_COL = "municipio_de_domicilio_de_la_ies"
INSTITUTION_ID_COL = "codigo_de_la_institucion"


def filter_bogota(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter rows to institutions domiciled in Bogotá.
    Uses partial string match to handle variants like 'Bogotá D.C.'.
    """
    before = len(df)
    mask = df[MUNICIPALITY_COL].str.contains("bogot", case=False, na=False)
    df = df[mask].copy()
    logger.info("Bogota filter: %d -> %d rows (dropped %d).", before, len(df), before - len(df))
    return df


def aggregate_by_institution_year(
    df: pd.DataFrame,
    metric_column: str,
    semester_col: str | None = None,
) -> pd.DataFrame:
    """
    Aggregate the metric column by institution and year (and semester if present).

    Args:
        df: Filtered Silver DataFrame.
        metric_column: Column to sum ('numero_de_docentes' or equivalent).
        semester_col: If provided and present in df, include in groupby keys.

    Returns:
        Aggregated DataFrame with one row per (institution × year [× semester]).
    """
    group_keys = [INSTITUTION_ID_COL, IES_COL, "ano"]

    if semester_col and semester_col in df.columns:
        group_keys.append(semester_col)
        logger.debug("Aggregating with semester granularity.")

    # Drop rows with null in the metric or any key column before aggregating
    df = df.dropna(subset=[metric_column] + [INSTITUTION_ID_COL])

    df_grouped = (
        df.groupby(group_keys, as_index=False)[metric_column]
        .sum()
    )

    # Rename IES column to a stable short name for downstream joins
    df_grouped = df_grouped.rename(columns={IES_COL: "institucion"})

    logger.info(
        "Aggregated to %d rows on keys %s.", len(df_grouped), group_keys
    )
    return df_grouped