"""
End-to-end dry-run test — no database required.
Validates the full ingestion → clean → silver → KPI → SUE chain
against the real SNIES Excel files in data/raw/.

Run with:
    uv run python tests/dry_run.py
"""

import logging
import sys
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)-8s | %(name)s | %(message)s",
    stream=sys.stdout,
)

from src.utils.config import SNIES_FILES, RAW_DATA_DIR
from src.ingestion.loader import load_source_file
from src.processing.cleaning import normalize_columns
from src.processing.docentes import process_docentes
from src.processing.estudiantes import process_estudiantes
from src.processing.data_quality import antijoin_report
from src.processing.sue_classifier import add_sue_flag

print("\n" + "="*60)
print("  SNIES Pipeline — Dry Run (no DB)")
print("="*60 + "\n")

all_doc, all_est = [], []

for year in SNIES_FILES:
    cfg = SNIES_FILES[year]

    raw_doc = load_source_file(year, "docentes", cfg["docentes"])
    raw_est = load_source_file(year, "estudiantes", cfg["estudiantes"])

    raw_doc = normalize_columns(raw_doc)
    raw_est = normalize_columns(raw_est)

    sil_doc = process_docentes(raw_doc, year)
    sil_est = process_estudiantes(raw_est, year)

    all_doc.append(sil_doc)
    all_est.append(sil_est)
    print(f"  {year} -> docentes: {len(sil_doc)} rows | estudiantes: {len(sil_est)} rows")

print()

df_doc = pd.concat(all_doc, ignore_index=True)
df_est = pd.concat(all_est, ignore_index=True)

print("--- Anti-join report ---")
antijoin_report(df_doc, df_est, "codigo_de_la_institucion")

merge_keys = ["codigo_de_la_institucion", "ano"]
if "semestre" in df_doc.columns and "semestre" in df_est.columns:
    merge_keys.append("semestre")

df = df_est.merge(df_doc, on=merge_keys, how="inner", suffixes=("_est", "_doc"))

# Resolve institution name after merge
if "institucion_est" in df.columns:
    df["institucion"] = df["institucion_est"]
    df = df.drop(columns=["institucion_est", "institucion_doc"], errors="ignore")

df["relacion_estudiantes_por_docente"] = (
    df["numero_de_estudiantes_matriculados"] /
    df["numero_de_docentes"].replace(0, float("nan"))
).round(2)

df = add_sue_flag(df, "institucion")

print("\n=== GOLD LAYER PREVIEW (top 15 by ratio, 2022) ===\n")
preview = (
    df[df["ano"] == 2022]
    [["institucion", "ano", "numero_de_estudiantes_matriculados", "numero_de_docentes", "relacion_estudiantes_por_docente", "es_sue"]]
    .sort_values("relacion_estudiantes_por_docente", ascending=False)
    .head(15)
)
print(preview.to_string(index=False))

print("\n=== SUMMARY ===")
print(f"  Total fact rows  : {len(df)}")
print(f"  Years covered    : {sorted(df['ano'].unique().tolist())}")
print(f"  SUE institutions : {df.drop_duplicates('codigo_de_la_institucion')['es_sue'].sum()}")
print(f"  KPI range        : {df['relacion_estudiantes_por_docente'].min():.1f} – {df['relacion_estudiantes_por_docente'].max():.1f}")
print()
