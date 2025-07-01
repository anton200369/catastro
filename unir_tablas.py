import pandas as pd

BIEN_GROUPED = 'bien_inmueble_grouped.xlsx'
TITULAR_GROUPED = 'titular_bien_inmueble_grouped.xlsx'
LIXO_GROUPED = 'lixo_padron_grouped.xlsx'
COINCIDENCIAS_OUT = 'coincidencias_iniciales.xlsx'
DNI_CALLE_SAME_OUT = 'dni_calle_same.xlsx'


def main():
    # Load grouped tables
    bien = pd.read_excel(BIEN_GROUPED, dtype=str)
    titular = pd.read_excel(TITULAR_GROUPED, dtype=str)
    lixo = pd.read_excel(LIXO_GROUPED, dtype=str)

    # Merge on id_parcela and miembro so that rows align within each parcel
    merged = pd.merge(
        bien,
        titular,
        on=["id_parcela", "miembro"],
        how="outer",

        suffixes=("_bien", "_tit")
    

    )

    merged = merged.sort_values(["id_parcela", "miembro"])

    # Build the full reference from union columns
    required = {"id_parcela", "numero_responsables", "id_ctr1", "id_ctr2"}
    if required.issubset(merged.columns):
        merged["ref_completa"] = (
            merged["id_parcela"].astype(str)
            + merged["numero_responsables"].astype(int).astype(str).str.zfill(4)
            + merged["id_ctr1"].astype(str)
            + merged["id_ctr2"].astype(str)
        )
    else:
        merged["ref_completa"] = pd.NA

    # Determine references that are present in Padron_Lixo
    refs_union = set(merged["ref_completa"].dropna())
    coincidencias = lixo[lixo["id_fullref"].isin(refs_union)]
    coincidencias = coincidencias.copy()

    # Remove rows from the union that have a matching reference
    merged = merged[~merged["ref_completa"].isin(coincidencias["id_fullref"])]
    # Determine references that are present in Padron_Lixo
    refs_lixo = set(lixo["id_fullref"].dropna())
    coincidencias = lixo[lixo["id_fullref"].isin(merged["id_fullref"].dropna())]

    # Remove rows from the union that have a matching reference
    merged = merged[~merged["id_fullref"].isin(refs_lixo)]

    # --------------------------------------------------------------
    # Nuevas coincidencias por DNI y codigo de via
    # --------------------------------------------------------------
    # Si lixo no tiene columna codvia, intenta obtenerla a partir de
    # los nombres de via presentes en las tablas de union.
    if "codvia" not in lixo.columns:
        map_bien = bien[["nombre_bien", "codvia_bien"]].dropna().drop_duplicates()
        map_bien.columns = ["nombre", "codvia"]
        map_tit = titular[["domicilio_actual", "codvia_tit"]].dropna().drop_duplicates()
        map_tit.columns = ["nombre", "codvia"]
        mapping = pd.concat([map_bien, map_tit]).drop_duplicates("nombre")
        lixo = pd.merge(lixo, mapping, left_on="nombre_final", right_on="nombre", how="left")
        lixo.drop(columns=["nombre"], inplace=True)

    dni_calle_matches = []
    if "codvia" in lixo.columns:
        lixo_reset = lixo.reset_index().rename(columns={"index": "idx_lixo"})
        merged_reset = merged.reset_index().rename(columns={"index": "idx_union"})
        for cv_col in ["codvia_tit", "codvia_bien"]:
            if cv_col in merged_reset.columns:
                temp = pd.merge(
                    lixo_reset,
                    merged_reset[["idx_union", "dni_tit", cv_col, "id_fullref"]],
                    left_on=["nif", "codvia"],
                    right_on=["dni_tit", cv_col],
                    how="inner",
                    suffixes=("", "_union"),
                )
                if not temp.empty:
                    dni_calle_matches.append(temp)

    if dni_calle_matches:
        matches = pd.concat(dni_calle_matches, ignore_index=True)
        matches = matches.drop_duplicates(subset=["idx_lixo", "idx_union"])

        if "id_fullref_union" in matches.columns:
            matches.rename(columns={"id_fullref_union": "ref_union"}, inplace=True)
        else:
            matches.rename(columns={"id_fullref": "ref_union"}, inplace=True)

        output_cols = [c for c in lixo.columns if c != "id_fullref"] + ["ref_union"]
        dni_calle_same = matches[output_cols]
        dni_calle_same.rename(columns={"ref_union": "id_fullref"}, inplace=True)
        dni_calle_same.to_excel(DNI_CALLE_SAME_OUT, index=False)

        lixo = lixo.drop(matches["idx_lixo"]).reset_index(drop=True)
        merged = merged.drop(matches["idx_union"]).reset_index(drop=True)

    # Insert visual separator column between datasets
    merged["sep_bien_tit"] = ""

    bien_cols = bien.columns.tolist()
    tit_cols = [c for c in titular.columns if c not in ["id_parcela", "miembro"]]

    column_order = (
        bien_cols
        + ["sep_bien_tit"]
        + tit_cols
    )
    column_order = [c for c in column_order if c in merged.columns]
    merged = merged.reindex(columns=column_order)

    merged.to_excel("union_grouped.xlsx", index=False)
    coincidencias.to_excel(COINCIDENCIAS_OUT, index=False)

    # Count number of rows per id_parcela in the merged result
    counts = merged.groupby("id_parcela").size().reset_index(name="num_filas")
    counts.to_excel("conteo_filas_por_id.xlsx", index=False)


if __name__ == "__main__":
    main()
