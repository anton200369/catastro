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
        suffixes=("_bien", "_tit")
    )

    merged = merged.sort_values(["id_parcela", "miembro"])

    # Determine references that are present in Padron_Lixo
    refs_lixo = set(lixo["id_fullref"].dropna())
    coincidencias = lixo[lixo["id_fullref"].isin(merged["id_fullref"].dropna())]

    # Remove rows from the union that have a matching reference
    merged = merged[~merged["id_fullref"].isin(refs_lixo)]

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
