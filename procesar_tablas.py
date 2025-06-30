import pandas as pd
import yaml

CONFIG_FILE = 'unionbienes-titulares.yaml'
BIEN_FILE = 'bien_inmueble.xlsx'
TITULAR_FILE = 'titular_bien_inmueble.xlsx'


def load_config(path=CONFIG_FILE):
    with open(path, 'r', encoding='utf8') as f:
        data = yaml.safe_load(f)
    return data['columns']


def preparar_tabla(path, mapping):
    df = pd.read_excel(path, dtype=str)
    df = df[list(mapping.keys())]
    rename_map = {k: v['as'] for k, v in mapping.items()}
    df = df.rename(columns=rename_map)
    return df.sort_values('id_parcela')


def mostrar_grupos(df, nombre):
    print(f"\nGrupos de {nombre} por id_parcela")
    for i, (parcel, grupo) in enumerate(df.groupby('id_parcela')):
        print(f"--- id_parcela: {parcel} ---")
        print(grupo)
        print()
        if i >= 2:  # muestra solo los tres primeros grupos
            break


def main():
    columnas = load_config()
    bien = preparar_tabla(BIEN_FILE, columnas['bien_inmueble'])
    titular = preparar_tabla(TITULAR_FILE, columnas['titular_bien_inmueble'])

    bien.to_csv('bien_inmueble_grouped.csv', index=False)
    titular.to_csv('titular_bien_inmueble_grouped.csv', index=False)

    mostrar_grupos(bien, 'bien_inmueble')
    mostrar_grupos(titular, 'titular_bien_inmueble')


if __name__ == '__main__':
    main()
