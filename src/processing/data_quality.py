"""
Data quality checks applied at the Silver layer ingestion boundary.

All checks are non-blocking by default (they log warnings and emit metrics
rather than raising). Set STRICT_DQ=true in env to make them raise on failure
— useful for CI validation.

Emitted metrics (returned as a dict) feed into the Prefect run summary.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)
STRICT_DQ = os.getenv("STRICT_DQ", "false").lower() == "true"


def _warn_or_raise(msg: str) -> None:
    if STRICT_DQ:
        raise ValueError(f"[DQ STRICT] {msg}")
    logger.warning("[DQ] %s", msg)


def check_nulls(df: pd.DataFrame, key_cols: list[str], context: str = "") -> dict:
    """Flag rows with nulls in key columns."""
    metrics = {}
    for col in key_cols:
        if col not in df.columns:
            continue
        null_count = df[col].isna().sum()
        metrics[f"nulls_{col}"] = int(null_count)
        if null_count:
            _warn_or_raise(
                f"[{context}] {null_count} null(s) in '{col}'. "
                "These rows will be excluded from aggregation."
            )
    return metrics


def check_duplicates(
    df: pd.DataFrame,
    key_cols: list[str],
    context: str = "",
) -> dict:
    """
    Detect duplicate (institution, year [, semester]) combos before aggregation.
    Duplicates at this stage indicate overlapping SNIES source records.
    """
    available_keys = [c for c in key_cols if c in df.columns]
    dupes = df[df.duplicated(subset=available_keys, keep=False)]
    count = len(dupes)
    if count:
        _warn_or_raise(
            f"[{context}] {count} duplicate rows on keys {available_keys}. "
            "Aggregation (groupby+sum) will handle this, but verify source data."
        )
    return {"duplicate_rows": count}


def check_kpi_range(
    df: pd.DataFrame,
    kpi_col: str,
    kpi_min: float,
    kpi_max: float,
    context: str = "",
) -> dict:
    """Flag KPI values outside the expected realistic range."""
    if kpi_col not in df.columns:
        return {}
    out_of_range = df[
        (df[kpi_col] < kpi_min) | (df[kpi_col] > kpi_max)
    ]
    count = len(out_of_range)
    if count:
        _warn_or_raise(
            f"[{context}] {count} row(s) with '{kpi_col}' outside "
            f"[{kpi_min}, {kpi_max}]:\n"
            + out_of_range[["codigo_de_la_institucion", kpi_col]].to_string()
        )
    return {"kpi_out_of_range": count}


def check_negative_values(
    df: pd.DataFrame, metric_col: str, context: str = ""
) -> dict:
    """Negative student/faculty counts are data errors."""
    if metric_col not in df.columns:
        return {}
    neg = (df[metric_col] < 0).sum()
    if neg:
        _warn_or_raise(
            f"[{context}] {neg} negative value(s) in '{metric_col}'. "
            "These will distort KPI calculations."
        )
    return {f"negative_{metric_col}": int(neg)}


def antijoin_report(
    df_left: pd.DataFrame,
    df_right: pd.DataFrame,
    key: str,
    left_label: str = "docentes",
    right_label: str = "estudiantes",
) -> dict:
    """
    Identify institutions present in one dataset but not the other.
    Inner join silently drops these — this makes the loss visible.
    """
    left_ids = set(df_left[key].dropna())
    right_ids = set(df_right[key].dropna())
    only_left = left_ids - right_ids
    only_right = right_ids - left_ids
    if only_left:
        logger.warning(
            "%d institution(s) in %s but not %s (will be dropped by inner join): %s",
            len(only_left), left_label, right_label, sorted(only_left)[:10],
        )
    if only_right:
        logger.warning(
            "%d institution(s) in %s but not %s (will be dropped by inner join): %s",
            len(only_right), right_label, left_label, sorted(only_right)[:10],
        )
    return {
        f"only_in_{left_label}": len(only_left),
        f"only_in_{right_label}": len(only_right),
    }


def run_silver_dq(
    df: pd.DataFrame,
    dataset_type: str,
    metric_col: str,
    year: int,
) -> dict:
    """
    Run the full DQ suite on a Silver-layer DataFrame.
    Returns a metrics dict for Prefect run summaries.
    """
    context = f"{dataset_type}_{year}"
    metrics: dict = {"year": year, "dataset": dataset_type, "rows": len(df)}

    metrics.update(
        check_nulls(df, ["codigo_de_la_institucion", metric_col], context)
    )
    metrics.update(
        check_duplicates(
            df,
            ["codigo_de_la_institucion", "ano"],
            context,
        )
    )
    metrics.update(check_negative_values(df, metric_col, context))

    logger.info("[DQ] %s %s: %s", dataset_type, year, metrics)
    return metrics
