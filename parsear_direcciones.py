import pandas as pd
import re

try:
    from postal.parser import parse_address as libpostal_parse
except ImportError:  # libpostal not installed
    libpostal_parse = None

def _parse_with_libpostal(text):
    comps = dict(libpostal_parse(text))
    return {
        'via': comps.get('road'),
        'numero': comps.get('house_number'),
        'letra': comps.get('unit'),
        'escalera': comps.get('staircase'),
        'planta': comps.get('level'),
        'puerta': comps.get('door'),
    }

_regex = re.compile(
    r"^(?P<via>[^0-9]+)?\s*(?P<numero>\d+)?\s*(?P<letra>[A-Za-z])?" \
    r"(?:\s+ESC(?:ALERA)?\s*(?P<escalera>\w+))?" \
    r"(?:\s+PL(?:ANTA)?\s*(?P<planta>\w+))?" \
    r"(?:\s+P(?:UERTA|TA)?\s*(?P<puerta>\w+))?",
    re.IGNORECASE,
)

def _parse_with_regex(text):
    if not isinstance(text, str):
        text = '' if pd.isna(text) else str(text)
    m = _regex.search(text.strip())
    result = {c: None for c in ['via','numero','letra','escalera','planta','puerta']}
    if m:
        for k,v in m.groupdict().items():
            if v:
                result[k] = v.strip()
        if not result['via']:
            result['via'] = text.strip()
    else:
        result['via'] = text.strip()
    return result

def parse_address(text):
    if libpostal_parse:
        return _parse_with_libpostal(text)
    return _parse_with_regex(text)

def build_full_address(via, numero, letra, escalera, planta, puerta):
    parts = [via, numero, letra, escalera, planta, puerta]
    parts = [str(p).strip() for p in parts if isinstance(p, str) and p.strip()]
    return ' '.join(parts)

def normalize_parsed(row):
    parts = []
    if row.get('via'):
        parts.append(row['via'].upper())
    if row.get('numero'):
        parts.append(str(row['numero']).upper())
    if row.get('letra'):
        parts.append(str(row['letra']).upper())
    if row.get('escalera'):
        parts.append(f"ESC {row['escalera'].upper()}")
    if row.get('planta'):
        parts.append(f"PL {row['planta'].upper()}")
    if row.get('puerta'):
        parts.append(f"PT {row['puerta'].upper()}")
    return ' '.join(parts)

def process_df(df, mapping, prefix):
    full_addr = df.apply(
        lambda r: build_full_address(
            r.get(mapping.get('via')),
            r.get(mapping.get('numero')),
            r.get(mapping.get('letra')),
            r.get(mapping.get('escalera')),
            r.get(mapping.get('planta')),
            r.get(mapping.get('puerta')),
        ),
        axis=1,
    )
    parsed = full_addr.apply(parse_address).apply(pd.Series)
    parsed_cols = {c: f"{prefix}_{c}" for c in parsed.columns}
    parsed.rename(columns=parsed_cols, inplace=True)
    norm = parsed.apply(normalize_parsed, axis=1)
    df[f'direccion_normalizada_{prefix}'] = norm
    df = pd.concat([df, parsed], axis=1)
    return df

def main():
    bien = pd.read_excel('bienes.xlsx', dtype=str)
    titular = pd.read_excel('titulares.xlsx', dtype=str)
    lixo = pd.read_excel('lixo.xlsx', dtype=str)

    bien = process_df(
        bien,
        {
            'via': 'nombre_bien',
            'numero': 'numero_bien',
            'letra': 'letra_bien',
            'escalera': 'escalera_bien',
            'planta': 'planta_bien',
            'puerta': 'puerta_bien',
        },
        'bien',
    )

    titular = process_df(
        titular,
        {
            'via': 'domicilio_actual',
            'numero': 'numero_tit',
            'letra': 'letra_tit',
            'escalera': 'escalera',
            'planta': 'planta',
            'puerta': 'puerta',
        },
        'tit',
    )

    lixo = process_df(
        lixo,
        {
            'via': 'nombre_final',
            'numero': 'numero_final',
            'letra': None,
            'escalera': 'escalera_final',
            'planta': 'planta_final',
            'puerta': 'puerta_final',
        },
        'lixo',
    )

    bien.to_excel('bienes_parseados.xlsx', index=False)
    titular.to_excel('titulares_parseados.xlsx', index=False)
    lixo.to_excel('lixo_parseados.xlsx', index=False)

    conflictos = bien[['id_fullref','direccion_normalizada_bien']]
    conflictos = conflictos.merge(
        titular[['id_fullref','direccion_normalizada_tit']],
        on='id_fullref', how='outer'
    ).merge(
        lixo[['id_fullref','direccion_normalizada_lixo']],
        on='id_fullref', how='outer'
    )

    def hay_conflicto(row):
        addrs = [row.get('direccion_normalizada_bien'),
                 row.get('direccion_normalizada_tit'),
                 row.get('direccion_normalizada_lixo')]
        addrs = [a for a in addrs if isinstance(a, str) and a]
        return len(addrs) >= 2 and len(set(addrs)) > 1

    conflictos = conflictos[conflictos.apply(hay_conflicto, axis=1)]
    conflictos.to_excel('conflictos_direcciones.xlsx', index=False)

if __name__ == '__main__':
    main()
