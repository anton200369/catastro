# nom_normalizer.py
"""Normaliza columnas textuales (calles, barrios, etc.) con Splink + DuckDB.

Función principal
-----------------
normalize_columns(df, cols, key_cols=..., ...)
→  devuelve (df_con_normalizadas, df_conflictos)
"""
from __future__ import annotations

import re
import unicodedata
import hashlib
from pathlib import Path
from typing import Iterable, Tuple
from rapidfuzz.distance import JaroWinkler
import pandas as pd
from splink.duckdb.linker import DuckDBLinker     
from splink.duckdb import comparison_library as cl   




# --- helpers -------------------------------------------------------------
RX_PREF = re.compile(
    r"^(CALLE|CL|C/|AVENIDA|AVDA\.?|AV/|CAMINO|CMNO\.?|PASEO|PS\.?)\s+",
    flags=re.I,
)

def canon(s: str | None) -> str:
    """Convierte ‘Av. Constitución’ → ‘CONSTITUCION’ (mayús, sin tildes, sin prefijo)."""
    if not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    s = re.sub(r"\s+", " ", s).upper().strip()
    s = RX_PREF.sub("", s)
    return s

def hash_id(value: str) -> str:
    """Hash corto para ID opcional."""
    return hashlib.md5(value.encode()).hexdigest()[:12]

    """no necesitamos hash para el ID en principio"""


    # --- núcleo Splink -------------------------------------------------------
def _link_single_column(
    df: pd.DataFrame,
    col: str,
    key_cols: list[str],
    prob_auto: float,
    prob_review: float,
    master_dir: Path,
) -> Tuple[pd.Series, pd.DataFrame]:
    """
    • Devuelve la Serie con el valor normalizado para <col>.
    • Devuelve también un DataFrame con los pares conflictivos (“revisión manual”).
    """
    master_dir.mkdir(exist_ok=True, parents=True)
    cat_path = master_dir / f"{col}.parquet"

    # 1) Cargar o crear catálogo maestro ----------------------------------
    if cat_path.exists():
        cat = pd.read_parquet(cat_path)
        df_full = pd.concat([df[[col] + key_cols], cat], ignore_index=True)
    else:
        df_full = df[[col] + key_cols].copy()

    # 2) Pre-normalización ligera (canon) ---------------------------------
    df_full["canon"] = df_full[col].map(canon)
    for k in key_cols:
        df_full[k] = df_full[k].fillna("")

    # 3) Configuración de Splink ------------------------------------------
    settings = {
    "link_type": "dedupe_only",
    "unique_id_column_name": "__rid",

    # 🔄  NUEVA CLAVE – misma lista de reglas
    "blocking_rules_to_generate_predictions": [
        "l.num_municipio = r.num_municipio AND substr(l.canon,1,4) = substr(r.canon,1,4)"
    ],

    # (opcional) si quieres ver cuántas comparaciones ahorras:
    "blocking_rule_for_training": "l.num_municipio = r.num_municipio",

    "comparisons": [
        cl.levenshtein_at_thresholds("canon", [1, 2, 3])
    ],
    "retain_intermediate_calculation_columns": False,
    "max_iterations": 3,
}

    df_full = df_full.reset_index(drop=True).assign(__rid=lambda x: x.index.astype(str))
    linker = DuckDBLinker(df_full, settings, connection=":memory:")

    # 4) Entrenamiento EM e inferencia ------------------------------------
    linker.estimate_parameters_using_expectation_maximisation()
    preds = linker.predict(threshold_match_probability=prob_review).as_pandas_dataframe()

    # 5) Agrupar en clusters únicos ---------------------------------------
    clusters = linker.cluster_pairwise_predictions_at_threshold(
        preds, threshold=prob_review
    )
    cluster_map = (
        clusters.set_index("cluster_id")
        .agg({"__root_id": "first"})
        .to_dict()["__root_id"]
    )
    df_full["cluster_id"] = df_full["__rid"].map(cluster_map)

    # Canon representativo = valor más frecuente en el cluster
    canon_rep = (
        df_full.groupby("cluster_id")["canon"]
        .agg(lambda s: s.value_counts().index[0])
        .to_dict()
    )
    df_full["canon_rep"] = df_full["cluster_id"].map(canon_rep)

    # 6) Actualizar catálogo maestro --------------------------------------
    master = (
        df_full.drop_duplicates(subset="cluster_id")[
            [col, "canon_rep"] + key_cols
        ]
        .rename(columns={"canon_rep": f"{col}_norm"})
    )
    master.to_parquet(cat_path, index=False)

    # 7) Serie resultante para las filas originales -----------------------
    norm_series = df_full.loc[: len(df) - 1, "canon_rep"]

    # 8) Construir tabla de conflictos ------------------------------------
    conflicts = (
        preds[preds.match_probability.between(prob_review, prob_auto)]
        .merge(df_full[[col, "__rid"]], left_on="unique_id_l", right_on="__rid")
        .merge(
            df_full[[col, "__rid"]],
            left_on="unique_id_r",
            right_on="__rid",
            suffixes=("_l", "_r"),
        )
        [[f"{col}_l", f"{col}_r", "match_probability"]]
        .rename(columns={f"{col}_l": "valor_izq", f"{col}_r": "valor_der"})
        .sort_values("match_probability", ascending=False)
        .reset_index(drop=True)
    )

    return norm_series, conflicts



# --- API principal -------------------------------------------------------
def normalize_columns(
    df: pd.DataFrame,
    cols: Iterable[str],
    *,
    key_cols: list[str] | None = None,
    prob_auto: float = 0.97,
    prob_review: float = 0.90,
    master_dir: str | Path = "master",
    audit_dir: str | Path = "audit",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    • `cols` .......... lista de columnas a normalizar (una o varias)
    • `key_cols` ...... columnas de bloqueo (municipio, provincia…)
    • `prob_auto` ..... umbral ≥ → auto-merge
    • `prob_review` ... umbral intermedio → bandeja de revisión
    Devuelve (DataFrame con <col>_norm, DataFrame de conflictos).
    """

    try:
        key_cols = key_cols or []
        master_dir, audit_dir = Path(master_dir), Path(audit_dir)
        audit_dir.mkdir(exist_ok=True, parents=True)

        conflicts_all = []
        df_out = df.copy()

        for col in cols:
            norm, conf = _link_single_column(
                df_out, col, key_cols, prob_auto, prob_review, master_dir
            )
            df_out[f"{col}_norm"] = norm
            if not conf.empty:
                conf.insert(0, "columna", col)
                conflicts_all.append(conf)

        conflicts_df = (
            pd.concat(conflicts_all, ignore_index=True)
            if conflicts_all
            else pd.DataFrame(columns=["columna", "valor_izq", "valor_der", "match_probability"])
        )

        if not conflicts_df.empty:
            out_path = audit_dir / f"conflictos_{pd.Timestamp.now():%Y%m%d_%H%M%S}.xlsx"
            conflicts_df.to_excel(out_path, index=False)
            print(f"[INFO] Conflictos guardados en → {out_path}")

        return df_out, conflicts_df, df


    except Exception as e:
        import traceback, sys
        print("\n[ERROR] normalize_columns ha fallado:", e, file=sys.stderr)
        traceback.print_exc()
        raise 






