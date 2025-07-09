import pandas as pd

UNION_FILE = "tabla_referencias_catastro.xlsx"
LIXO_FILE = "tabla_referencias_lixo.xlsx"

OUT_DNI = "candidatos_por_dni.xlsx"
OUT_DIR = "candidatos_por_direccion.xlsx"
OUT_EXC = "excepciones.xlsx"
OUT_DNI_DIR = "candidatos_dni_y_direccion.xlsx"


def _first_valid(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def main():
    try:
        union = pd.read_excel(UNION_FILE, dtype=str)
        lixo = pd.read_excel(LIXO_FILE, dtype=str)
    except FileNotFoundError:
        return

    # Filas de lixo sin referencia asignada
    lixo_nr = lixo[lixo["id_fullref"].isna() | (lixo["id_fullref"].astype(str).str.strip() == "")].copy()
    lixo_idx = lixo_nr.reset_index().rename(columns={"index": "idx_lixo"})

    # --- Candidatos por DNI ---
    if "dni_tit" in union.columns and "nif" in lixo_idx.columns:
        matches = pd.merge(
            lixo_idx[["idx_lixo", "nif"]],
            union[["id_fullref", "dni_tit"]],
            left_on="nif",
            right_on="dni_tit",
            how="left",
        )
        agg = (
            matches.dropna(subset=["id_fullref"])
            .groupby("idx_lixo")["id_fullref"]
            .agg(lambda s: ", ".join(sorted(set(s))))
            .reset_index()
            .rename(columns={"id_fullref": "refs_candidatas_dni"})
        )
        agg["num_candidatos_dni"] = agg["refs_candidatas_dni"].apply(
            lambda x: len(x.split(", "))
        )
        result_dni_all = lixo_idx.merge(agg, on="idx_lixo", how="left")
    else:
        result_dni_all = lixo_idx.copy()
        result_dni_all["refs_candidatas_dni"] = pd.NA
        result_dni_all["num_candidatos_dni"] = 0

    # --- DNI candidates that also match by address keys ---
    keys_order = [
        "codbloq_num_letra_esc_pl_pt",
        "codbloq_num_letra_esc_pl",
        "codbloq_num_letra_esc",
        "codbloq_num_letra",
        "codbloq_num",
        #"codbloq",
        #"cod",
    ]

    def candidate_matches(row, ref):
        cand_rows = union[union["id_fullref"] == ref]
        if cand_rows.empty:
            return False
        for _, urow in cand_rows.iterrows():
            for k in keys_order:
                lixo_val = row.get(f"dir_lixo_{k}")
                union_val = _first_valid(urow.get(f"dir_bien_{k}"), urow.get(f"dir_tit_{k}"))
                if (
                    isinstance(lixo_val, str)
                    and lixo_val.strip()
                    and isinstance(union_val, str)
                    and union_val.strip()
                    and lixo_val.strip() == union_val.strip()
                ):
                    return True
        return False

    refs_dni_dir = []
    for _, r in result_dni_all.iterrows():
        if not isinstance(r.get("refs_candidatas_dni"), str):
            refs_dni_dir.append(pd.NA)
            continue
        cand_list = [c.strip() for c in r["refs_candidatas_dni"].split(",") if c.strip()]
        matched = []
        for cand in cand_list:
            if candidate_matches(r, cand):
                matched.append(cand)
        if matched:
            refs_dni_dir.append(", ".join(sorted(set(matched))))
        else:
            refs_dni_dir.append(pd.NA)

    result_dni_all["refs_candidatas_dni_dir"] = refs_dni_dir
    result_dni_all["num_candidatos_dni_dir"] = result_dni_all[
        "refs_candidatas_dni_dir"
    ].apply(lambda x: len(x.split(", ")) if isinstance(x, str) else 0)

    candidatos_dni_dir = result_dni_all.dropna(subset=["refs_candidatas_dni_dir"]).copy()
    dni_dir_idxs = set(candidatos_dni_dir["idx_lixo"].unique())
    candidatos_dni_dir = candidatos_dni_dir.drop(columns=["idx_lixo"], errors="ignore")
    candidatos_dni_dir.to_excel(OUT_DNI_DIR, index=False)

    candidatos_dni = (
        result_dni_all
        .dropna(subset=["refs_candidatas_dni"])
        .loc[~result_dni_all["idx_lixo"].isin(dni_dir_idxs)]
        .drop(columns=["idx_lixo"], errors="ignore")
    )
    candidatos_dni.to_excel(OUT_DNI, index=False)

    # --- Candidatos por dirección completa ---
    key = "codbloq_num_letra_esc_pl_pt"
    lixo_col = f"dir_lixo_{key}"
    ref_bien = f"dir_bien_{key}"
    ref_tit = f"dir_tit_{key}"

    if lixo_col in lixo_idx.columns and (ref_bien in union.columns or ref_tit in union.columns):
        union["addr_key"] = (
            union.apply(
                lambda r: _first_valid(r.get(ref_bien), r.get(ref_tit)), axis=1
            )
            .fillna("")
            .astype(str)
        )
        lixo_idx["addr_key"] = lixo_idx[lixo_col].fillna("").astype(str)

        matches = pd.merge(
            lixo_idx[["idx_lixo", "addr_key"]],
            union[["id_fullref", "addr_key"]],
            on="addr_key",
            how="left",
        )
        agg = (
            matches.dropna(subset=["id_fullref"])
            .groupby("idx_lixo")["id_fullref"]
            .agg(lambda s: ", ".join(sorted(set(s))))
            .reset_index()
            .rename(columns={"id_fullref": "refs_candidatas_dir"})
        )
        agg["num_candidatos_dir"] = agg["refs_candidatas_dir"].apply(
            lambda x: len(x.split(", "))
        )
        result_dir_all = lixo_idx.merge(agg, on="idx_lixo", how="left")
    else:
        result_dir_all = lixo_idx.copy()
        result_dir_all["refs_candidatas_dir"] = pd.NA
        result_dir_all["num_candidatos_dir"] = 0

    candidatos_dir = (
        result_dir_all
        .dropna(subset=["refs_candidatas_dir"])
        .loc[~result_dir_all["idx_lixo"].isin(dni_dir_idxs)]
        .drop(columns=["idx_lixo", "addr_key"], errors="ignore")
    )
    candidatos_dir.to_excel(OUT_DIR, index=False)

    # --- Filas sin candidatos por ninguno de los criterios ---
    merge_all = result_dni_all[["idx_lixo", "refs_candidatas_dni"]].merge(
        result_dir_all[["idx_lixo", "refs_candidatas_dir"]],
        on="idx_lixo",
        how="outer",
    )
    sin_candidatos = merge_all[
        merge_all["refs_candidatas_dni"].isna() & merge_all["refs_candidatas_dir"].isna()
    ]
    if not sin_candidatos.empty:
        excepciones = lixo_idx.merge(sin_candidatos[["idx_lixo"]], on="idx_lixo", how="inner")
        excepciones = excepciones.drop(columns=["idx_lixo", "addr_key"], errors="ignore")
        excepciones.to_excel(OUT_EXC, index=False)
    else:
        pd.DataFrame().to_excel(OUT_EXC, index=False)


if __name__ == "__main__":
    main()