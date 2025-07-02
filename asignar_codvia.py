import pandas as pd
import re

BIEN_GROUPED = 'bienes_parseados.xlsx'
LIXO_GROUPED = 'lixo_parseados.xlsx'
OUTPUT = 'lixo_parseados.xlsx'
CONFLICTS = 'lixo_conflictos.xlsx'


def normalize(text: str) -> str:
    if pd.isna(text):
        return ''
    text = re.sub(r'\(.*?\)', '', text)
    text = text.replace(',', ' ')
    text = text.upper()
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_lixo_name(name: str):
    name = normalize(name)
    if '-' in name:
        parts = [p.strip() for p in name.split('-') if p.strip()]
        parish = parts[0]
        area = '-'.join(parts[1:]) if len(parts) > 1 else ''
    else:
        parish = name
        area = ''
    return area, parish


def build_mapping(bien_df: pd.DataFrame):
    bien_df = bien_df.dropna(subset=['codvia_bien', 'nombre_bien'])
    bien_df['norm_name'] = bien_df['nombre_bien'].apply(normalize)
    mapping = dict()
    for _, row in bien_df[['norm_name', 'codvia_bien']].drop_duplicates().iterrows():
        mapping[row['norm_name']] = row['codvia_bien']
    return mapping


def main():
    bien = pd.read_excel(BIEN_GROUPED, dtype=str)
    lixo = pd.read_excel(LIXO_GROUPED, dtype=str)

    mapping = build_mapping(bien)

    norm_names = []
    codvias = []
    conflicts = []

    for name in lixo['lixo_via']:
        area, parish = parse_lixo_name(name)
        norm = normalize(f'{area}-{parish}') if area else normalize(parish)
        norm_names.append(norm)
        codvias.append(mapping.get(norm))
        if norm not in mapping:
            conflicts.append({'nombre_final': name, 'nombre_normalizado': norm})

    lixo['nombre_normalizado'] = norm_names
    lixo['codvia_asignado'] = codvias

    lixo.to_excel(OUTPUT, index=False)

    if conflicts:
        pd.DataFrame(conflicts).drop_duplicates().to_excel(CONFLICTS, index=False)

if __name__ == '__main__':
    main()