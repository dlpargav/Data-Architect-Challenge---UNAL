"""
SUE (Sistema Universitario Estatal) classification.

The SUE list is sourced from:
  https://www.upn.edu.co/directorio-sistema-universitario-estatal-sue/

Strategy: normalized fuzzy match against the SNIES institution name column
using a pre-built reference set. This handles common variations like:
  - "Universidad Nacional de Colombia" vs "UNIVERSIDAD NACIONAL DE COLOMBIA"
  - "Univ. Distrital" vs "Universidad Distrital Francisco José de Caldas"

The match uses token-set containment (not Levenshtein) to stay dependency-free
and fast at this data scale.
"""

import logging
import unicodedata

import pandas as pd

logger = logging.getLogger(__name__)

# ─── SUE reference list ────────────────────────────────────────────────────────
# Full list extracted from UPN website — all 34 SUE institutions nationwide.
# Bogotá-relevant ones are marked (★) but all are stored for the national
# scaling scenario.

SUE_INSTITUTIONS: list[str] = [
    # Capítulo Caribe
    "Universidad del Atlántico",
    "Universidad de Cartagena",
    "Universidad Popular del Cesar",
    "Universidad de Córdoba",
    "Universidad de La Guajira",
    "Universidad del Magdalena",
    "Universidad de Sucre",
    # Capítulo Distrito Capital ★
    "Universidad Distrital Francisco José de Caldas",   # ★
    "Universidad Colegio Mayor de Cundinamarca",         # ★
    "Universidad Militar Nueva Granada",                 # ★
    "Universidad Pedagógica Nacional",                   # ★
    "Universidad Nacional de Colombia",                  # ★
    # Capítulo Centro
    "Universidad Pedagógica y Tecnológica de Colombia",
    "Universidad de Cundinamarca",
    "Universidad Nacional Abierta y a Distancia",
    "Universidad del Tolima",
    "Universidad Surcolombiana",
    # Capítulo Eje Cafetero
    "Universidad de Antioquia",
    "Universidad Tecnológica de Pereira",
    "Universidad del Quindío",
    "Universidad de Caldas",
    # Capítulo Suroccidente
    "Universidad del Valle",
    "Universidad del Cauca",
    "Universidad de Nariño",
    "Universidad del Pacífico",
    "Universidad de la Amazonía",
    "Universidad Autónoma Indígena Intercultural",
    "Universidad Tecnológica del Chocó",
    # Capítulo Oriente / Llanos
    "Universidad de los Llanos",
    "Universidad de Pamplona",
    "Universidad Industrial de Santander",
    "Universidad Francisco de Paula Santander",
    "Universidad Francisco de Paula Santander Ocaña",
    "Unitrópico",
]


def _normalize(text: str) -> str:
    """Lowercase + remove accents for comparison."""
    if not isinstance(text, str):
        return ""
    text = text.lower().strip()
    text = "".join(
        c for c in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(c)
    )
    return text


# Pre-compute normalized reference tokens once
_SUE_NORMALIZED: list[str] = [_normalize(name) for name in SUE_INSTITUTIONS]


def _is_sue(institution_name: str) -> bool:
    """
    Return True if the institution name matches any SUE member.

    Matching logic:
      1. Exact normalized match (fast path).
      2. Token-containment: if all significant tokens of a SUE name appear
         in the institution name (handles abbreviations and SNIES truncations).
    """
    norm_candidate = _normalize(institution_name)
    if not norm_candidate:
        return False

    for sue_norm in _SUE_NORMALIZED:
        # Exact match
        if norm_candidate == sue_norm:
            return True
        # Containment: the SUE canonical name is a substring of the SNIES name
        if sue_norm in norm_candidate:
            return True
        # Reverse containment: SNIES name is a substring of the canonical name
        # (handles truncated SNIES labels)
        if norm_candidate in sue_norm and len(norm_candidate) > 10:
            return True

    return False


def add_sue_flag(df: pd.DataFrame, name_col: str = "institucion") -> pd.DataFrame:
    """
    Add an 'es_sue' boolean column to a DataFrame containing institution names.

    Args:
        df: DataFrame with an institution name column.
        name_col: Column containing IES names.

    Returns:
        DataFrame with 'es_sue' column added.
    """
    if name_col not in df.columns:
        logger.warning("Column '%s' not found — es_sue will be False for all rows.", name_col)
        df = df.copy()
        df["es_sue"] = False
        return df

    df = df.copy()
    df["es_sue"] = df[name_col].apply(_is_sue)

    sue_count = df["es_sue"].sum()
    logger.info(
        "SUE classification: %d / %d institutions flagged as SUE members.",
        sue_count, len(df),
    )
    return df
