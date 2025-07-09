import pandas as pd
import os

CANDIDATOS_FILE = 'candidatos_dni_y_direccion.xlsx'
ASIGNACIONES_FILE = 'tabla_asignaciones.xlsx'
LIXO_REF_FILE = 'tabla_referencias_lixo.xlsx'
CATASTRO_REF_FILE = 'tabla_referencias_catastro.xlsx'


def cargar_tabla(path):
    """Load an Excel file if it exists, otherwise return an empty DataFrame."""
    if os.path.exists(path):
        return pd.read_excel(path, dtype=str)
    return pd.DataFrame()


def obtener_columna_referencia(df):
    """Return the name of a reference column if present."""
    posibles = ['referencia', 'id_fullref', 'ref_cat', 'ref']
    for col in posibles:
        if col in df.columns:
            return col
    return None


def filtrar_candidatos_unicos(df):
    """Return rows with a single candidate based on precomputed counts."""
    if 'num_candidatos_dni_dir' in df.columns:
        conteo = pd.to_numeric(df['num_candidatos_dni_dir'], errors='coerce')
        return df[conteo == 1].reset_index(drop=True)

    # Fall back to identifying unique keys from the data itself
    posibles_claves = ['id_parcela', 'dni', 'direccion', 'entrada_id']
    claves = [c for c in posibles_claves if c in df.columns]
    if not claves:
        claves = [df.columns[0]]

    conteo = df.groupby(claves).size()
    unicos = conteo[conteo == 1].index

    return df.set_index(claves).loc[unicos].reset_index()


def filtrar_coincidencias_lixo(lixo_df, cat_df):
    """Select rows in lixo_df whose reference is present in cat_df."""
    if lixo_df.empty or cat_df.empty:
        return pd.DataFrame()

    col_lixo = obtener_columna_referencia(lixo_df)
    col_cat = obtener_columna_referencia(cat_df)
    if not col_lixo or not col_cat:
        return pd.DataFrame()

    refs_cat = set(cat_df[col_cat].dropna())
    mask = lixo_df[col_lixo].notna() & lixo_df[col_lixo].isin(refs_cat)
    return lixo_df[mask]


def main():
    candidatos = cargar_tabla(CANDIDATOS_FILE)
    asignaciones = cargar_tabla(ASIGNACIONES_FILE)
    lixo_refs = cargar_tabla(LIXO_REF_FILE)
    cat_refs = cargar_tabla(CATASTRO_REF_FILE)

    nuevas_listas = []

    if not candidatos.empty:
        unicos = filtrar_candidatos_unicos(candidatos)
        nuevas_listas.append(unicos)

    coincidencias = filtrar_coincidencias_lixo(lixo_refs, cat_refs)
    if not coincidencias.empty:
        lixo_refs = lixo_refs.drop(coincidencias.index)
        nuevas_listas.append(coincidencias)
        lixo_refs.to_excel(LIXO_REF_FILE, index=False)

    if nuevas_listas:
        resultado = pd.concat([asignaciones] + nuevas_listas, ignore_index=True)
        resultado.to_excel(ASIGNACIONES_FILE, index=False)
        print(f"Se añadieron {sum(len(df) for df in nuevas_listas)} filas a {ASIGNACIONES_FILE}")
    else:
        print("No hay filas nuevas para asignar")


if __name__ == '__main__':
    main()