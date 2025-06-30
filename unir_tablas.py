import pandas as pd

BIEN_GROUPED = 'bien_inmueble_grouped.xlsx'
TITULAR_GROUPED = 'titular_bien_inmueble_grouped.xlsx'
LIXO_GROUPED = 'lixo_padron_grouped.xlsx'


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

    merged = pd.merge(
        merged,
        lixo,
        on=["id_fullref", "miembro"],
        how="left"
    )

    merged = merged.sort_values(["id_parcela", "miembro"])
    merged.to_excel("union_grouped.xlsx", index=False)

    # Count number of rows per id_parcela in the merged result
    counts = merged.groupby("id_parcela").size().reset_index(name="num_filas")
    counts.to_excel("conteo_filas_por_id.xlsx", index=False)


if __name__ == "__main__":
    main()
