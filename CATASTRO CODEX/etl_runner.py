#!/usr/bin/env python3
# src/etl_runner.py
# ---------------------------------------------------------------------------
# Runner genérico para procesar cualquier YAML declarativo de recorte, join
# y salida.  Diseñado para funcionar con los ejemplos que hemos tratado.
# ---------------------------------------------------------------------------

from __future__ import annotations
import importlib
import subprocess
from pathlib import Path
from datetime import datetime
import hashlib
import yaml
import click
import pandas as pd

from pathlib import Path
import importlib
import pandas as pd
import yaml, click, json, datetime


# ──────────────────────────── utilidades ────────────────────────────────── #
def short_md5(path: Path, length: int = 8) -> str:
    return hashlib.md5(path.read_bytes()).hexdigest()[:length]


def stamp(fmt="%Y%m%d_%H%M%S"):
    return datetime.datetime.utcnow().strftime(fmt)


def expand_tokens(template: str, *, base: str, date: str, hash_: str) -> str:
    return (
        template.replace("{base}", base)
                .replace("{date}", date)
                .replace("{hash}", hash_)
    )


# ────────────────────── lectura y recorte de columnas ───────────────────── #
def apply_column_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:    #solo deja columnas renombradas(las que usamos)#
    keep = [col for col in mapping if col in df.columns]
    cleansed = df[keep].copy()
    cleansed.rename(columns={src: mapping[src]["as"] for src in keep}, inplace=True)
    return cleansed


def load_dataframe(path: Path, sheet=0) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path, sheet_name=sheet, dtype=str).fillna("")
    return pd.read_csv(path, dtype=str).fillna("")


# ───────────────────────────── unificación ─────────────────────────────── #

#según decisiones del yaml, pilla la columna de la tabla que queramos para ponerla como la común en la tabla nueva#

def unify_duplicates(df: pd.DataFrame, rules: list[dict]) -> pd.DataFrame:
    for rule in rules:
        col = rule["name"]
        left, right = f"{col}_x", f"{col}_y"
        if left not in df.columns or right not in df.columns:
            continue
        prefer = rule.get("prefer", "left")
        df[col] = df[left] if prefer == "left" else df[right]
        df.drop(columns=[left, right], inplace=True)
    return df


# ────────────────────────── proceso de un YAML ──────────────────────────── #
# etl_runner.py  — función completa y autocontenida


def process_yaml(yaml_path: Path, run_date: str) -> None:
    """Carga YAML, ejecuta ETL y GUARDA siempre un CSV en output.dir"""

    cfg = yaml.safe_load(yaml_path.read_text("utf-8"))
    print("\n=== CLAVES RAÍZ ===", list(cfg.keys()))
    print("=== builders ===", json.dumps(cfg.get("builders"), indent=2, ensure_ascii=False))

    # ── 1 · Cargar datasets ────────────────────────────────────────────
    dfs: dict[str, pd.DataFrame] = {}
    for ds_name, ds_spec in cfg["datasets"].items():
        if "path" in ds_spec:                                      # compat. antiguo
            file_path = sorted(Path().glob(ds_spec["path"]))[0]
        else:
            file_path = sorted(Path(ds_spec["dir"]).glob(ds_spec["pattern"]))[0]

        df = load_dataframe(file_path)
        df = apply_column_mapping(df, cfg["columns"][ds_name])

        dfs[ds_name] = df
        click.echo(f"   {ds_name}: {len(df):,} filas tras recorte")

    # ── 2 · JOIN ──────────────────────────────────────────────────────
    jcfg = cfg["join"]
    joined = (
        dfs[jcfg["left"]]
        .merge(
            dfs[jcfg["right"]],
            how      = jcfg["how"],
            on       = jcfg["on"],
            validate = jcfg.get("validate"),
            suffixes = ("_x", "_y"),
        )
    )
    click.secho(f"   JOIN ok → {len(joined):,} filas", fg="green")

    # ── 3 · Unify duplicadas (opcional) ───────────────────────────────
    joined = unify_duplicates(joined, cfg.get("unify_cols", []))

    # ── 4 · Builders (opcional) ───────────────────────────────────────
    for b_name, b_cfg in cfg.get("builders", {}).items():
        print("[DEBUG] builder", b_name, "cfg =", b_cfg)
       
        #module, func_name = b_cfg["using"].rsplit(".", 1)

        print("[DEBUG] usando =", repr(b_cfg["using"]))
        try:
            module, func_name = b_cfg["using"].rsplit(".", 1)
            print("[DEBUG] módulo =", module, "  función =", func_name)
            mod = importlib.import_module(module)
            print("[DEBUG] importlib OK →", mod)
            func = getattr(mod, func_name)
            print("[DEBUG] getattr OK →", func)
        except Exception as e:
            import traceback, sys
            print("\n[ERROR] localizando builder:", e, "\n", file=sys.stderr)
            traceback.print_exc()
            sys.exit(1)
        #func   = getattr(importlib.import_module(module), func_name)
        params = b_cfg.get("params", {})
        mode   = b_cfg.get("apply", "row")

        print(f"[DEBUG] builder {b_name} apply={mode}")

        if mode == "row":
            joined[b_name] = joined.apply(lambda r: func(r, **params), axis=1)

        elif mode == "vector":
            joined[b_name] = func(joined[b_cfg["source"]], **params)

        elif mode == "frame":
            try:
                joined, conflicts_df, _ = func(joined, **params)

                print(
        "[DEBUG] builder", b_name,
        "ha VUELTO – tipo:", type(joined),
        "shape:", getattr(joined, "shape", None),
        "conflictos:", len(conflicts_df)
    )
            except Exception as e:
                import traceback, sys
                print("\n[ERROR] builder via_norm ha fallado →", e, "\n", file=sys.stderr)
                traceback.print_exc()      # traza completa
                sys.exit(1)                # detén la ejecución para ver la traza

        else:
            raise ValueError(f"Modo apply desconocido: {mode}")

        if not isinstance(joined, pd.DataFrame):
            raise RuntimeError(f"Builder {b_name} devolvió objeto inesperado: {type(joined)}")

    # ── 5 · Guardar SIEMPRE la salida ─────────────────────────────────
    out_dir  = Path(cfg["output"]["dir"])
    out_dir.mkdir(parents=True, exist_ok=True)

    out_name = cfg["output"]["name"].format(date=run_date)
    out_path = out_dir / out_name

    print("[DEBUG] Shape final:", joined.shape)
    print("[DEBUG] Guardando en:", out_path)

    joined.to_csv(out_path, index=False, encoding="utf-8")
    click.secho(f"✔  Escrito {out_path}", fg="bright_green")



# ─────────────────────── CLI principal (Click) ──────────────────────────── #
@click.command()
@click.option("--yaml", "yaml_file", type=click.Path(exists=True),
              help="Ruta a un único YAML.")
@click.option("--cfg_dir", type=click.Path(exists=True), default="config",
              help="Carpeta con varios YAML.")
@click.option("--date", "run_date", default=stamp(),
              help="Sello de fecha para los nombres de fichero.")
def main(yaml_file: str | None, cfg_dir: str, run_date: str):
    """Runner declarativo para YAMLs de recorte, join y export."""
    if yaml_file:
        process_yaml(Path(yaml_file), run_date)
    else:
        for yml in sorted(Path(cfg_dir).glob("*.yml")):
            click.echo(click.style(f"\n▶ Procesando {yml.name}", bold=True))
            process_yaml(yml, run_date)


if __name__ == "__main__":
    main()
