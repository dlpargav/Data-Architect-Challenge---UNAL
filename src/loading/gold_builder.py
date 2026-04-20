"""
Gold layer builder — assembles the star schema from processed Silver DataFrames.

Flow:
  1. Upsert dim_institucion (with SUE flag)
  2. Upsert dim_periodo
  3. Join docentes + estudiantes, compute KPI
  4. Upsert fact_capacidad_academica

All upserts use INSERT ... ON CONFLICT DO UPDATE (PostgreSQL) so the pipeline
is fully idempotent — safe to re-run without duplicating data.
"""

import logging
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.loading.database import get_engine, get_session
from src.loading.models import DimInstitucion, DimPeriodo, FactCapacidadAcademica
from src.processing.sue_classifier import add_sue_flag
from src.utils.config import KPI_MIN, KPI_MAX

logger = logging.getLogger(__name__)


# ─── Dimension helpers ────────────────────────────────────────────────────────

def upsert_dim_institucion(df_combined: pd.DataFrame) -> None:
    """
    Upsert institution dimension from the combined (docentes + estudiantes) data.
    Uses the SNIES institution code as the natural key.
    """
    # Deduplicate — one row per institution code, take the last known name
    institutions = (
        df_combined[["codigo_de_la_institucion", "institucion"]]
        .drop_duplicates(subset=["codigo_de_la_institucion"])
        .dropna(subset=["codigo_de_la_institucion"])
        .copy()
    )

    institutions = add_sue_flag(institutions, name_col="institucion")

    engine = get_engine()
    with engine.begin() as conn:
        for _, row in institutions.iterrows():
            stmt = pg_insert(DimInstitucion).values(
                codigo_institucion=int(row["codigo_de_la_institucion"]),
                nombre_institucion=str(row["institucion"]),
                es_sue=bool(row["es_sue"]),
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["codigo_institucion"],
                set_={
                    "nombre_institucion": stmt.excluded.nombre_institucion,
                    "es_sue": stmt.excluded.es_sue,
                },
            )
            conn.execute(stmt)

    logger.info("dim_institucion: %d institutions upserted.", len(institutions))


def upsert_dim_periodo(anos: list[int], semesters: list[int | None]) -> dict:
    """
    Upsert period dimension rows and return a mapping of (ano, semestre) → id_periodo.
    """
    engine = get_engine()
    periodo_map: dict = {}

    with engine.begin() as conn:
        for ano in anos:
            for sem in semesters:
                stmt = pg_insert(DimPeriodo).values(ano=ano, semestre=sem)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_periodo_ano_semestre",
                    set_={"ano": stmt.excluded.ano},
                )
                result = conn.execute(stmt)

                # Fetch the id for the mapping
                row = conn.execute(
                    text(
                        "SELECT id_periodo FROM gold.dim_periodo "
                        "WHERE ano = :ano AND semestre IS NOT DISTINCT FROM :sem"
                    ),
                    {"ano": ano, "sem": sem},
                ).fetchone()
                if row:
                    periodo_map[(ano, sem)] = row[0]

    logger.info("dim_periodo: %d periods upserted.", len(periodo_map))
    return periodo_map


# ─── Fact builder ─────────────────────────────────────────────────────────────

def build_gold_layer(
    df_docentes_all: pd.DataFrame,
    df_estudiantes_all: pd.DataFrame,
) -> pd.DataFrame:
    """
    Build and persist the Gold star schema from the full multi-year Silver data.

    Args:
        df_docentes_all: Concatenated Silver docentes across all years.
        df_estudiantes_all: Concatenated Silver estudiantes across all years.

    Returns:
        The final Gold fact DataFrame (for inspection/logging).
    """
    # ── Determine granularity ─────────────────────────────────────────────────
    has_semester_doc = "semestre" in df_docentes_all.columns
    has_semester_est = "semestre" in df_estudiantes_all.columns
    has_semester = has_semester_doc and has_semester_est
    merge_keys = ["codigo_de_la_institucion", "ano"]
    if has_semester:
        merge_keys.append("semestre")
        logger.info("Gold build: semester-level granularity.")
    else:
        logger.info("Gold build: annual granularity.")

    # ── Join docentes + estudiantes ───────────────────────────────────────────
    df_merged = df_estudiantes_all.merge(
        df_docentes_all,
        on=merge_keys,
        how="inner",
        suffixes=("_est", "_doc"),
    )

    # Resolve institution name column after merge
    if "institucion_est" in df_merged.columns:
        df_merged["institucion"] = df_merged["institucion_est"]
        df_merged = df_merged.drop(columns=["institucion_est", "institucion_doc"], errors="ignore")

    # ── Anti-join reporting ───────────────────────────────────────────────────
    from src.processing.data_quality import antijoin_report
    antijoin_report(df_docentes_all, df_estudiantes_all, "codigo_de_la_institucion")

    # ── KPI calculation ───────────────────────────────────────────────────────
    df_merged["relacion_estudiantes_por_docente"] = (
        df_merged["numero_de_estudiantes_matriculados"] /
        df_merged["numero_de_docentes"].replace(0, pd.NA)  # avoid division by zero
    ).round(2)

    # Flag suspicious KPI values
    out_of_range = df_merged[
        (df_merged["relacion_estudiantes_por_docente"] < KPI_MIN) |
        (df_merged["relacion_estudiantes_por_docente"] > KPI_MAX)
    ]
    if len(out_of_range):
        logger.warning(
            "%d rows with KPI outside [%s, %s] — review these institutions.",
            len(out_of_range), KPI_MIN, KPI_MAX,
        )

    # ── Upsert dimensions ─────────────────────────────────────────────────────
    upsert_dim_institucion(df_merged)

    anos = sorted(df_merged["ano"].unique().tolist())
    sems = sorted(df_merged["semestre"].unique().tolist()) if has_semester else [None]
    periodo_map = upsert_dim_periodo(anos, sems)

    # ── Upsert fact rows ──────────────────────────────────────────────────────
    engine = get_engine()
    now = datetime.now(timezone.utc)
    rows_loaded = 0

    with engine.begin() as conn:
        for _, row in df_merged.iterrows():
            sem_key = int(row["semestre"]) if has_semester else None
            id_periodo = periodo_map.get((int(row["ano"]), sem_key))
            if id_periodo is None:
                logger.warning("No period found for ano=%s semestre=%s — skipping.", row["ano"], sem_key)
                continue

            stmt = pg_insert(FactCapacidadAcademica).values(
                codigo_institucion=int(row["codigo_de_la_institucion"]),
                id_periodo=id_periodo,
                total_estudiantes_matriculados=int(row["numero_de_estudiantes_matriculados"]),
                total_docentes=int(row["numero_de_docentes"]),
                relacion_estudiantes_por_docente=row["relacion_estudiantes_por_docente"],
                fecha_procesamiento=now,
            )
            stmt = stmt.on_conflict_do_update(
                constraint="uq_fact_inst_periodo",
                set_={
                    "total_estudiantes_matriculados": stmt.excluded.total_estudiantes_matriculados,
                    "total_docentes": stmt.excluded.total_docentes,
                    "relacion_estudiantes_por_docente": stmt.excluded.relacion_estudiantes_por_docente,
                    "fecha_procesamiento": stmt.excluded.fecha_procesamiento,
                },
            )
            conn.execute(stmt)
            rows_loaded += 1

    logger.info("fact_capacidad_academica: %d rows upserted.", rows_loaded)
    return df_merged
