"""
Central configuration for the SNIES Academic Capacity Pipeline.

To add a new period:
  1. Add a new year block to SNIES_FILES with the correct URLs from
     https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/
  2. Adjust skiprows if SNIES changes the header format for that year.
  3. Re-run the pipeline — it will download, process, and load automatically.
"""

import os
from pathlib import Path

# ─── Project paths ────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DATA_DIR = BASE_DIR / "data" / "processed"
GOLD_DATA_DIR = BASE_DIR / "data" / "gold"

# Create dirs if they don't exist (safe at import time)
for _d in (RAW_DATA_DIR, PROCESSED_DATA_DIR, GOLD_DATA_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ─── Database ─────────────────────────────────────────────────────────────────
# In Docker the host is "postgres"; locally override with DATABASE_URL env var.
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://snies:snies@localhost:5432/snies_db",
)

# ─── SNIES source files ───────────────────────────────────────────────────────
# URLs retrieved from:
#   https://snies.mineducacion.gov.co/portal/ESTADISTICAS/Bases-consolidadas/
#
# Replace PLACEHOLDER_* with the real article IDs once you have them.
# Pattern: https://snies.mineducacion.gov.co/1778/articles-<ID>_recurso.xlsx
#
# skiprows: number of metadata/header rows to skip before the actual data row.
# This value can differ per year — adjust if the schema validation step
# raises a MissingColumnError for a given file.

SNIES_FILES: dict = {
    2022: {
        "docentes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-416249_recurso.xlsx",
            "local_name": "Docentes 2022.xlsx",
            "skiprows": 7,
            "sheet_name": 0,
        },
        "estudiantes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-416244_recurso.xlsx",
            "local_name": "Estudiantes 2022.xlsx",
            "skiprows": 8,
            "sheet_name": 0,
        },
    },
    2023: {
        "docentes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-421822_recurso.xlsx",
            "local_name": "Docentes 2023.xlsx",
            "skiprows": 5,
            "sheet_name": "1.",
        },
        "estudiantes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-421539_recurso.xlsx",
            "local_name": "Estudiantes 2023.xlsx",
            "skiprows": 5,
            "sheet_name": "1.",
        },
    },
    2024: {
        "docentes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-425156_recurso.xlsx",
            "local_name": "Docentes 2024.xlsx",
            "skiprows": 5,
            "sheet_name": "1.",
        },
        "estudiantes": {
            "url": "https://snies.mineducacion.gov.co/1778/articles-425151_recurso.xlsx",
            "local_name": "Estudiantes 2024.xlsx",
            "skiprows": 5,
            "sheet_name": "1.",
        },
    },
}

# ─── Column contracts ─────────────────────────────────────────────────────────
# Minimum set of normalized column names expected after cleaning.
# The pipeline raises MissingColumnError if any are absent — fast failure
# is preferable to silent downstream data corruption.

DOCENTES_REQUIRED_COLS = {
    "codigo_de_la_institucion",
    "institucion_de_educacion_superior_(ies)",
    "municipio_de_domicilio_de_la_ies",
    "numero_de_docentes",          # after rename from no._de_docentes
}

ESTUDIANTES_REQUIRED_COLS = {
    "codigo_de_la_institucion",
    "institucion_de_educacion_superior_(ies)",
    "municipio_de_domicilio_de_la_ies",
    "numero_de_estudiantes_matriculados",   # after rename from matriculados
}

# Optional columns (kept if present, drives semester-level granularity)
SEMESTER_COL_CANDIDATES = ["semestre", "periodo", "semester"]

# ─── KPI guard-rails ──────────────────────────────────────────────────────────
KPI_MIN = 1       # below this → flag as suspicious
KPI_MAX = 200     # above this → flag as suspicious
