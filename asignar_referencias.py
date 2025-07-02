import pandas as pd

UNION_FILE = 'union_grouped.xlsx'
LIXO_FILE = 'lixo_padron_codvia.xlsx'
COINCIDENCIAS_OUT = 'tabla_coincidencias.xlsx'
UNION_RESTANTE = 'union_restante.xlsx'
LIXO_RESTANTE = 'lixo_restante.xlsx'


def load_df(path):
    return pd.read_excel(path, dtype=str).fillna('')


def match_direct(union_df, lixo_df):
    refs_union = set(union_df['id_fullref'])
    mask = lixo_df['id_fullref'].isin(refs_union)
    direct = lixo_df[mask].copy()
    union_df = union_df[~union_df['id_fullref'].isin(direct['id_fullref'])].reset_index(drop=True)
    lixo_df = lixo_df[~mask].reset_index(drop=True)
    direct['origen'] = 'referencia_directa'
    return direct, union_df, lixo_df


def match_by_address(union_df, lixo_df):
    lixo_tmp = lixo_df.reset_index().rename(columns={'index': 'idx_lixo'})
    union_tmp = union_df.reset_index().rename(columns={'index': 'idx_union'})

    left_cols = ['nif', 'codvia_asignado', 'numero_final', 'escalera_final', 'planta_final', 'puerta_final']
    right_cols = ['dni_tit', 'codvia_bien', 'numero_bien', 'escalera_bien', 'planta_bien', 'puerta_bien']

    merged = pd.merge(
        lixo_tmp,
        union_tmp[right_cols + ['idx_union', 'id_fullref']],
        left_on=left_cols,
        right_on=right_cols,
        how='inner',
        suffixes=("", "_cat"),
    )

    if merged.empty:
        return pd.DataFrame(), union_df, lixo_df

    matched_lixo = lixo_df.loc[merged['idx_lixo']].copy()
    matched_lixo['id_fullref'] = merged['id_fullref_cat'].values
    matched_lixo['origen'] = 'direccion_dni'

    union_df = union_df.drop(merged['idx_union']).reset_index(drop=True)
    lixo_df = lixo_df.drop(merged['idx_lixo']).reset_index(drop=True)

    return matched_lixo, union_df, lixo_df


def main():
    union_df = load_df(UNION_FILE)
    lixo_df = load_df(LIXO_FILE)

    coincidencias = []

    direct, union_df, lixo_df = match_direct(union_df, lixo_df)
    coincidencias.append(direct)

    addr, union_df, lixo_df = match_by_address(union_df, lixo_df)
    if not addr.empty:
        coincidencias.append(addr)

    if coincidencias:
        tabla = pd.concat(coincidencias, ignore_index=True)
    else:
        tabla = pd.DataFrame(columns=lixo_df.columns.tolist() + ['origen'])

    tabla.to_excel(COINCIDENCIAS_OUT, index=False)
    union_df.to_excel(UNION_RESTANTE, index=False)
    lixo_df.to_excel(LIXO_RESTANTE, index=False)


if __name__ == '__main__':
    main()
