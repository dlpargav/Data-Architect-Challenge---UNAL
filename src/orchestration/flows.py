"""
Prefect orchestration — SNIES Academic Capacity Pipeline.

Why Prefect over Apache Airflow:
─────────────────────────────────────────────────────────────────────────────
  1. Zero-config scheduler: Prefect runs as a pure Python library. No separate
     metadata database, webserver, or worker infrastructure needed — a single
     `prefect server start` command gives you a full UI with run history.

  2. Native Python: tasks are plain functions decorated with @task. No DAG DSL
     or Jinja templates to learn. This fits a small, data-engineering-focused
     team much better than Airflow's operator model.

  3. Lightweight Docker footprint: Airflow requires 4+ services (webserver,
     scheduler, worker, postgres metadata DB). Prefect needs 1 (the server),
     and the pipeline container is self-contained.

  4. Built-in retries, logging, and observability out of the box with
     @task(retries=3, retry_delay_seconds=30).

  5. Migration path to Airflow: if the team grows and needs multi-tenant
     scheduling, the flow logic here maps 1:1 to an Airflow DAG — each @task
     becomes a PythonOperator. See README § Scaling Nationally.
─────────────────────────────────────────────────────────────────────────────

Run locally:
    uv run python -m src.orchestration.flows

Run via Prefect server:
    prefect server start &
    uv run python -m src.orchestration.flows
"""

import logging
from typing import Optional

import pandas as pd
from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

from src.utils.config import SNIES_FILES
from src.ingestion.loader import load_source_file
from src.processing.cleaning import normalize_columns
from src.processing.docentes import process_docentes
from src.processing.estudiantes import process_estudiantes
from src.loading.database import get_engine, init_db
from src.loading.bronze_loader import load_bronze_docentes, load_bronze_estudiantes
from src.loading.gold_builder import build_gold_layer


# ─── Tasks ────────────────────────────────────────────────────────────────────

@task(
    name="ingest-source-file",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=24),
    description="Download (or load from cache) a SNIES source file.",
)
def ingest_file(year: int, dataset_type: str) -> pd.DataFrame:
    logger = get_run_logger()
    file_config = SNIES_FILES[year][dataset_type]
    logger.info("Ingesting %s %d from %s", dataset_type, year, file_config["url"])
    df = load_source_file(year, dataset_type, file_config)
    logger.info("Loaded %d rows for %s %d", len(df), dataset_type, year)
    return df


@task(
    name="persist-bronze",
    description="Persist raw DataFrame to the Bronze layer.",
)
def persist_bronze(df: pd.DataFrame, year: int, dataset_type: str) -> None:
    logger = get_run_logger()
    logger.info("Loading bronze %s %d (%d rows)", dataset_type, year, len(df))
    if dataset_type == "docentes":
        load_bronze_docentes(df, year)
    else:
        load_bronze_estudiantes(df, year)


@task(
    name="normalize-and-clean",
    description="Normalize column names and apply cleaning rules.",
)
def normalize_and_clean(df: pd.DataFrame, dataset_type: str) -> pd.DataFrame:
    logger = get_run_logger()
    df = normalize_columns(df)
    logger.info("Normalized columns for %s", dataset_type)
    return df


@task(
    name="process-docentes",
    description="Silver transform: filter Bogotá, DQ checks, aggregate docentes.",
)
def run_process_docentes(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return process_docentes(df, year)


@task(
    name="process-estudiantes",
    description="Silver transform: filter Bogotá, DQ checks, aggregate estudiantes.",
)
def run_process_estudiantes(df: pd.DataFrame, year: int) -> pd.DataFrame:
    return process_estudiantes(df, year)


@task(
    name="build-gold",
    description="Join Silver tables, compute KPI, upsert Gold star schema.",
)
def run_build_gold(
    all_docentes: list[pd.DataFrame],
    all_estudiantes: list[pd.DataFrame],
) -> pd.DataFrame:
    logger = get_run_logger()
    df_doc = pd.concat(all_docentes, ignore_index=True)
    df_est = pd.concat(all_estudiantes, ignore_index=True)
    logger.info(
        "Building gold layer: %d docente rows + %d estudiante rows",
        len(df_doc), len(df_est),
    )
    return build_gold_layer(df_doc, df_est)


# ─── Flow ─────────────────────────────────────────────────────────────────────

@flow(
    name="snies-academic-capacity-pipeline",
    description=(
        "End-to-end pipeline: download SNIES microdata → Bronze → Silver → "
        "Gold star schema with student-to-teacher KPI and SUE classification."
    ),
    log_prints=True,
)
def run_pipeline(years: Optional[list[int]] = None) -> None:
    """
    Main orchestration flow.

    Args:
        years: List of years to process. Defaults to all years in SNIES_FILES.
               Pass a subset to backfill or re-process specific periods.

    Example:
        run_pipeline()           # all configured years
        run_pipeline([2024])     # only 2024 (useful for incremental loads)
    """
    years_to_process = years or list(SNIES_FILES.keys())
    logger = get_run_logger()
    logger.info("Pipeline started for years: %s", years_to_process)

    # ── Database setup ────────────────────────────────────────────────────────
    init_db()

    # ── Per-year ingestion and Silver processing ──────────────────────────────
    all_docentes: list[pd.DataFrame] = []
    all_estudiantes: list[pd.DataFrame] = []

    for year in years_to_process:
        # Ingestion
        raw_doc = ingest_file(year, "docentes")
        raw_est = ingest_file(year, "estudiantes")

        # Bronze persistence (raw data traceability)
        persist_bronze(raw_doc, year, "docentes")
        persist_bronze(raw_est, year, "estudiantes")

        # Normalize column names
        clean_doc = normalize_and_clean(raw_doc, "docentes")
        clean_est = normalize_and_clean(raw_est, "estudiantes")

        # Silver transformation
        silver_doc = run_process_docentes(clean_doc, year)
        silver_est = run_process_estudiantes(clean_est, year)

        all_docentes.append(silver_doc)
        all_estudiantes.append(silver_est)

    # ── Gold layer (cross-year) ───────────────────────────────────────────────
    df_gold = run_build_gold(all_docentes, all_estudiantes)
    logger.info(
        "Pipeline complete. Gold layer: %d fact rows.", len(df_gold)
    )


if __name__ == "__main__":
    run_pipeline()
