import pandas as pd
import yaml

CONFIG_FILE = 'unionbienes-titulares.yaml'
LIXO_CONFIG = 'columnslixo.yaml'

BIEN_FILE = 'bien_inmueble.xlsx'
TITULAR_FILE = 'titular_bien_inmueble.xlsx'
LIXO_FILE = 'Padron_Lixo.xlsx'


def load_config(path=CONFIG_FILE):
    with open(path, 'r', encoding='utf8') as f:
        data = yaml.safe_load(f)
    return data['columns']


def preparar_tabla(path, mapping, group_field='id_parcela'):
    df = pd.read_excel(path, dtype=str)
    df = df[list(mapping.keys())]
    rename_map = {k: v['as'] for k, v in mapping.items()}
    df = df.rename(columns=rename_map)

    # Construye la referencia completa si existen los componentes necesarios
    required = {'id_parcela', 'numero_responsables', 'id_ctr1', 'id_ctr2'}
    if required.issubset(df.columns):
        df['id_fullref'] = (
            df['id_parcela'] +
            df['numero_responsables'].astype(int).astype(str).str.zfill(4) +
            df['id_ctr1'] +
            df['id_ctr2']
        )

    df = df.sort_values(group_field)
    df['miembro'] = df.groupby(group_field).cumcount() + 1
    return df


def mostrar_grupos(df, nombre, group_field='id_parcela'):
    print(f"\nGrupos de {nombre} por {group_field}")
    for i, (key, grupo) in enumerate(df.groupby(group_field)):
        print(f"--- {group_field}: {key} ---")
        print(grupo)
        print()
        if i >= 2:  # muestra solo los tres primeros grupos
            break


def main():
    columnas_union = load_config(CONFIG_FILE)
    columnas_lixo = load_config(LIXO_CONFIG)

    bien = preparar_tabla(BIEN_FILE, columnas_union['bien_inmueble'])
    titular = preparar_tabla(TITULAR_FILE, columnas_union['titular_bien_inmueble'])
    lixo = preparar_tabla(LIXO_FILE, columnas_lixo['datos_catastro'], group_field='id_fullref')

    bien.to_excel('bien_inmueble_grouped.xlsx', index=False)
    titular.to_excel('titular_bien_inmueble_grouped.xlsx', index=False)
    lixo.to_excel('lixo_padron_grouped.xlsx', index=False)

    mostrar_grupos(bien, 'bien_inmueble')
    mostrar_grupos(titular, 'titular_bien_inmueble')
    mostrar_grupos(lixo, 'lixo_padron', group_field='id_fullref')


if __name__ == '__main__':
    main()
