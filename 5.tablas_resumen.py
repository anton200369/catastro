import pandas as pd

BIEN_PARSED = "bienes_parseados.xlsx"
TIT_PARSED = "titulares_parseados.xlsx"
LIXO_PARSED = "lixo_parseados.xlsx"

OUT_UNION = "tabla_referencias_catastro.xlsx"
OUT_LIXO = "tabla_referencias_lixo.xlsx"


def _existing(df, cols):
    """Return only columns present in df."""
    return [c for c in cols if c in df.columns]


def _build_union(bien, tit):
    # Keep all rows as-is without removing duplicates
    merged = pd.merge(bien, tit, on="id_fullref", how="inner")

    bien_keys = [
        "dir_bien_codbloq_num_letra_esc_pl_pt",
        "dir_bien_codbloq_num_letra_esc_pl",
        "dir_bien_codbloq_num_letra_esc",
        "dir_bien_codbloq_num_letra",
        "dir_bien_codbloq_num",
        "dir_bien_codbloq",
        "dir_bien_cod",
    ]

    tit_keys = [
        "dir_tit_codbloq_num_letra_esc_pl_pt",
        "dir_tit_codbloq_num_letra_esc_pl",
        "dir_tit_codbloq_num_letra_esc",
        "dir_tit_codbloq_num_letra",
        "dir_tit_codbloq_num",
        "dir_tit_codbloq",
        "dir_tit_cod",
    ]

    cols = [
        "id_fullref",
        "dni_tit",
        "nombre_apellidos_tit",
    ] + _existing(merged, bien_keys) + [
        "bien_via",
        "tit_via",
    ] + _existing(merged, tit_keys)

    cols = _existing(merged, cols)
    return merged[cols]


def _build_lixo(lixo):
    """Return lixo rows with reference, DNI, name, via and all key columns."""

    lixo_keys = [
        "dir_lixo_codbloq_num_letra_esc_pl_pt",
        "dir_lixo_codbloq_num_letra_esc_pl",
        "dir_lixo_codbloq_num_letra_esc",
        "dir_lixo_codbloq_num_letra",
        "dir_lixo_codbloq_num",
        "dir_lixo_codbloq",
        "dir_lixo_cod",
    ]

    for k in lixo_keys:
        if k not in lixo.columns:
            lixo[k] = pd.NA

    base_cols = ["id_fullref", "nif", "nombre_apell"]
    if "nombre_normalizado" in lixo.columns:
        base_cols.append("nombre_normalizado")
    elif "lixo_via" in lixo.columns:
        base_cols.append("nombre_normalizado")
        lixo = lixo.rename(columns={"lixo_via": "nombre_normalizado"})

    cols = base_cols + lixo_keys
    cols = _existing(lixo, cols)
    df = lixo[cols]
    return df


def main():
    try:
        bien = pd.read_excel(BIEN_PARSED, dtype=str)
        tit = pd.read_excel(TIT_PARSED, dtype=str)
        union = _build_union(bien, tit)
    except FileNotFoundError:
        union = None

    try:
        lixo = pd.read_excel(LIXO_PARSED, dtype=str)
    except FileNotFoundError:
        lixo = None

    if union is not None:
        union.to_excel(OUT_UNION, index=False)

    if lixo is not None:
        resumen_lixo = _build_lixo(lixo)
        resumen_lixo.to_excel(OUT_LIXO, index=False)


if __name__ == "__main__":
    main()