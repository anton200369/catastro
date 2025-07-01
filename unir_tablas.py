import pandas as pd

BIEN_GROUPED = 'bien_inmueble_grouped.xlsx'
TITULAR_GROUPED = 'titular_bien_inmueble_grouped.xlsx'
LIXO_GROUPED = 'lixo_padron_grouped.xlsx'
COINCIDENCIAS_OUT = 'coincidencias_iniciales.xlsx'


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
        suffixes=("_bien", "_tit"),
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
