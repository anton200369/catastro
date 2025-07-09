import pandas as pd

FILES = [
    ("bienes_parseados.xlsx", "bien"),
    ("titulares_parseados.xlsx", "tit"),
    ("lixo_parseados.xlsx", "lixo"),
]


def _join(parts):
    return " ".join(
        [str(p).strip().upper() for p in parts if isinstance(p, str) and str(p).strip()]
    )


def add_variants(df, prefix):
    cols = {
        "codvia": f"codvia_{prefix}",
        "bloque": f"bloque_{prefix}",
        "numero": f"{prefix}_numero",
        "letra": f"{prefix}_letra",
        "escalera": f"{prefix}_escalera",
        "planta": f"{prefix}_planta",
        "puerta": f"{prefix}_puerta",
    }

    def build_row(row):
        return [
            row.get(cols["codvia"]),
            row.get(cols["bloque"]),
            row.get(cols["numero"]),
            row.get(cols["letra"]),
            row.get(cols["escalera"]),
            row.get(cols["planta"]),
            row.get(cols["puerta"]),
        ]

    def compute_keys(row):
        parts = build_row(row)
        keys = [_join(parts[:i]) for i in range(len(parts), 0, -1)]
        return pd.Series(keys, index=new_cols)

    new_cols = [
        f"dir_{prefix}_codbloq_num_letra_esc_pl_pt",
        f"dir_{prefix}_codbloq_num_letra_esc_pl",
        f"dir_{prefix}_codbloq_num_letra_esc",
        f"dir_{prefix}_codbloq_num_letra",
        f"dir_{prefix}_codbloq_num",
        f"dir_{prefix}_codbloq",
        f"dir_{prefix}_cod",
    ]

    df[new_cols] = df.apply(compute_keys, axis=1)
    return df


def main():
    for path, prefix in FILES:
        try:
            df = pd.read_excel(path, dtype=str)
        except FileNotFoundError:
            continue
        df = add_variants(df, prefix)
        df.to_excel(path, index=False)


if __name__ == "__main__":
    main()