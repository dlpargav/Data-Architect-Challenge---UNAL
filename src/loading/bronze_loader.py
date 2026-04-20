"""
Bronze layer loader — persists raw DataFrames into bronze.* tables.

Design: truncate-and-reload per (year) partition.
Bronze tables are append-only from a business perspective, but for
re-runability we delete the year's existing rows before inserting new ones.
This avoids the complexity of full upsert on raw data while keeping the
layer idempotent.
"""

import logging

import pandas as pd
from sqlalchemy import text

from src.loading.database import get_engine
from src.processing.cleaning import normalize_columns

logger = logging.getLogger(__name__)

_BRONZE_COLS_DOCENTES = [
    "codigo_de_la_institucion",
    "institucion",
    "municipio_de_domicilio_de_la_ies",
    "numero_de_docentes",
    "semestre",
    "ano",
    "ingestion_timestamp",
]

_BRONZE_COLS_ESTUDIANTES = [
    "codigo_de_la_institucion",
    "institucion",
    "municipio_de_domicilio_de_la_ies",
    "numero_de_estudiantes_matriculados",
    "semestre",
    "ano",
    "ingestion_timestamp",
]


def _select_available(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Select only columns that exist in the DataFrame (optional cols may be absent)."""
    return df[[c for c in cols if c in df.columns]].copy()


def load_bronze_docentes(df_raw: pd.DataFrame, year: int) -> None:
    """Load raw docentes data into bronze.docentes_raw for the given year."""
    engine = get_engine()
    
    # 1. Normalize columns to match expected snake_case keys
    df = normalize_columns(df_raw)
    
    # 2. Map normalized SNIES names to the shorter Bronze schema names
    mapping = {
        "institucion_de_educacion_superior_(ies)": "institucion",
        "no._de_docentes": "numero_de_docentes",
    }
    df = df.rename(columns=mapping)
    
    # 3. Select subset defined in model
    df = _select_available(df, _BRONZE_COLS_DOCENTES)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM bronze.docentes_raw WHERE ano = :year"),
            {"year": year},
        )
        df.to_sql(
            "docentes_raw",
            conn,
            schema="bronze",
            if_exists="append",
            index=False,
            chunksize=1000,
        )
    logger.info("Bronze docentes %d: %d rows loaded.", year, len(df))


def load_bronze_estudiantes(df_raw: pd.DataFrame, year: int) -> None:
    """Load raw estudiantes data into bronze.estudiantes_raw for the given year."""
    engine = get_engine()
    
    # 1. Normalize columns
    df = normalize_columns(df_raw)
    
    # 2. Map normalized SNIES names to Bronze schema names
    mapping = {
        "institucion_de_educacion_superior_(ies)": "institucion",
        "numero_de_estudiantes_matriculados": "numero_de_estudiantes_matriculados", # for 2023+
        "matriculados_2022": "numero_de_estudiantes_matriculados",                   # for 2022
    }
    # Some years use 'matriculados' before normalization, which becomes 'matriculados'
    if "matriculados" in df.columns:
        df = df.rename(columns={"matriculados": "numero_de_estudiantes_matriculados"})
    
    df = df.rename(columns=mapping)
    
    # 3. Select subset
    df = _select_available(df, _BRONZE_COLS_ESTUDIANTES)

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM bronze.estudiantes_raw WHERE ano = :year"),
            {"year": year},
        )
        df.to_sql(
            "estudiantes_raw",
            conn,
            schema="bronze",
            if_exists="append",
            index=False,
            chunksize=1000,
        )
    logger.info("Bronze estudiantes %d: %d rows loaded.", year, len(df))
