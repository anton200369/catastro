import pandas as pd
import openpyxl

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

    # Mapeo de referencia a la fila del libro catastro
    try:
        wb = openpyxl.load_workbook(UNION_FILE, read_only=True)
        sheet_name = wb.sheetnames[0]
        wb.close()
    except Exception:
        sheet_name = "Sheet1"

    row_map = {}
    if "id_fullref" in union.columns:
        for idx, ref in enumerate(union["id_fullref"].fillna("")):
            if isinstance(ref, str) and ref.strip() and ref.strip() not in row_map:
                row_map[ref.strip()] = idx + 2

    def _linkify(refs: str):
        if not isinstance(refs, str):
            return refs
        parts = [p.strip() for p in refs.split(",") if p.strip()]
        linked = []
        for p in parts:
            row_num = row_map.get(p)
            if row_num is not None:
                linked.append(
                    f'=HYPERLINK("{UNION_FILE}#{sheet_name}!A{row_num}", "{p}")'
                )
            else:
                linked.append(p)
        return ", ".join(linked) if linked else pd.NA

    def _apply_links(df: pd.DataFrame) -> pd.DataFrame:
        for c in df.columns:
            if c.startswith("refs"):
                df[c] = df[c].apply(_linkify)
        return df

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

    def candidate_best_match(row, ref):
        cand_rows = union[union["id_fullref"] == ref]
        best_idx = None
        best_col = None
        for _, urow in cand_rows.iterrows():
            for idx, k in enumerate(keys_order):
                lixo_val = row.get(f"dir_lixo_{k}")
                if not isinstance(lixo_val, str) or not lixo_val.strip():
                    continue
                bien_val = urow.get(f"dir_bien_{k}")
                tit_val = urow.get(f"dir_tit_{k}")
                if (
                    isinstance(bien_val, str)
                    and bien_val.strip()
                    and lixo_val.strip() == bien_val.strip()
                ):
                    if best_idx is None or idx < best_idx:
                        best_idx = idx
                        best_col = f"dir_bien_{k}"
                    break
                if (
                    isinstance(tit_val, str)
                    and tit_val.strip()
                    and lixo_val.strip() == tit_val.strip()
                ):
                    if best_idx is None or idx < best_idx:
                        best_idx = idx
                        best_col = f"dir_tit_{k}"
                    break
        return best_idx, best_col

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

    # --- Best address key for DNI candidates ---
    best_refs = []
    best_cols = []
    best_keys = []
    for _, r in result_dni_all.iterrows():
        if not isinstance(r.get("refs_candidatas_dni"), str):
            best_refs.append(pd.NA)
            best_cols.append(pd.NA)
            best_keys.append(pd.NA)
            continue
        cand_list = [c.strip() for c in r["refs_candidatas_dni"].split(",") if c.strip()]
        info = []
        for cand in cand_list:
            idx, col = candidate_best_match(r, cand)
            if idx is not None:
                info.append((idx, cand, col))
        if not info:
            best_refs.append(pd.NA)
            best_cols.append(pd.NA)
            best_keys.append(pd.NA)
            continue
        best_idx = min(i[0] for i in info)
        best_cands = [c for i, c, _ in info if i == best_idx]
        best_refs.append(", ".join(sorted(set(best_cands))))
        best_keys.append(keys_order[best_idx] if best_idx is not None else pd.NA)
        if len(cand_list) == 1:
            col = next((c for i, _, c in info if i == best_idx), pd.NA)
            best_cols.append(col)
        else:
            best_cols.append(pd.NA)

    result_dni_all["refs_mejor_candidatos"] = best_refs
    result_dni_all["clave_mejor_candidato"] = best_keys
    result_dni_all["columna_coincidente"] = best_cols

    candidatos_dni_dir = result_dni_all.dropna(subset=["refs_candidatas_dni_dir"]).copy()
    dni_dir_idxs = set(candidatos_dni_dir["idx_lixo"].unique())
    candidatos_dni_dir = candidatos_dni_dir.drop(columns=["idx_lixo"], errors="ignore")
    candidatos_dni_dir = _apply_links(candidatos_dni_dir)
    candidatos_dni_dir.to_excel(OUT_DNI_DIR, index=False, engine="openpyxl")

    candidatos_dni = (
        result_dni_all
        .dropna(subset=["refs_candidatas_dni"])
        .loc[~result_dni_all["idx_lixo"].isin(dni_dir_idxs)]
        .drop(columns=["idx_lixo"], errors="ignore")
    )
    candidatos_dni = _apply_links(candidatos_dni)
    candidatos_dni.to_excel(OUT_DNI, index=False, engine="openpyxl")

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
        if "nombre_apellidos_tit" in union.columns:
            name_map = (
                union[["id_fullref", "nombre_apellidos_tit"]]
                .dropna(subset=["id_fullref", "nombre_apellidos_tit"])
                .drop_duplicates(subset=["id_fullref"])
                .set_index("id_fullref")
                ["nombre_apellidos_tit"]
            )

            def get_name(row):
                if (
                    isinstance(row.get("refs_candidatas_dir"), str)
                    and row.get("num_candidatos_dir") == 1
                ):
                    ref = row["refs_candidatas_dir"].strip()
                    return name_map.get(ref, pd.NA)
                return pd.NA

            result_dir_all["nombre_catastro"] = result_dir_all.apply(get_name, axis=1)
    else:
        result_dir_all = lixo_idx.copy()
        result_dir_all["refs_candidatas_dir"] = pd.NA
        result_dir_all["num_candidatos_dir"] = 0
        result_dir_all["nombre_catastro"] = pd.NA

    candidatos_dir = (
        result_dir_all
        .dropna(subset=["refs_candidatas_dir"])
        .loc[~result_dir_all["idx_lixo"].isin(dni_dir_idxs)]
        .drop(columns=["idx_lixo", "addr_key"], errors="ignore")
    )
    candidatos_dir = _apply_links(candidatos_dir)
    candidatos_dir.to_excel(OUT_DIR, index=False, engine="openpyxl")

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