import pandas as pd
import re

try:
    from postal.parser import parse_address as libpostal_parse
except ImportError:  # libpostal not installed
    libpostal_parse = None

def _parse_with_libpostal(text):
    """Return only the road component using libpostal."""
    comps = dict(libpostal_parse(text))
    return comps.get("road")

_regex = re.compile(
    r"^(?P<via>[^0-9]+)?\s*(?P<numero>[\w/-]+)?(?:\s+(?P<letra>[A-Za-z]))?" \
    r"(?:\s+ESC(?:ALERA)?\s*(?P<escalera>\w+))?" \
    r"(?:\s+PL(?:ANTA)?\s*(?P<planta>\w+))?" \
    r"(?:\s+P(?:UERTA|TA)?\s*(?P<puerta>\w+))?",
    re.IGNORECASE,
)

def _parse_with_regex(text):
    if not isinstance(text, str):
        text = '' if pd.isna(text) else str(text)
    m = _regex.search(text.strip())
    if m and m.group('via'):
        return m.group('via').strip()
    return text.strip()

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

    def _clean(value):
        if pd.isna(value):
            return ""
        return str(value).strip()

    via = _clean(row.get("via"))
    if via:
        parts.append(via.upper())

    numero = _clean(row.get("numero"))
    if numero:
        parts.append(numero.upper())

    letra = _clean(row.get("letra"))
    if letra:
        parts.append(letra.upper())

    esc = _clean(row.get("escalera"))
    if esc:
        parts.append(f"ESC {esc.upper()}")

    pl = _clean(row.get("planta"))
    if pl:
        parts.append(f"PL {pl.upper()}")

    pu = _clean(row.get("puerta"))
    if pu:
        parts.append(f"PT {pu.upper()}")
    return ' '.join(parts)

def process_df(df, mapping, prefix):
    full_addr = df.apply(
        lambda r: build_full_address(
            r.get(mapping.get("via")),
            r.get(mapping.get("numero")),
            r.get(mapping.get("letra")),
            r.get(mapping.get("escalera")),
            r.get(mapping.get("planta")),
            r.get(mapping.get("puerta")),
        ),
        axis=1,
    )

    via = full_addr.apply(parse_address)

    def orig(col):
        src = mapping.get(col)
        return df[src] if src and src in df.columns else pd.NA

    parsed = pd.DataFrame({
        f"{prefix}_via": via,
        f"{prefix}_numero": orig("numero"),
        f"{prefix}_letra": orig("letra"),
        f"{prefix}_escalera": orig("escalera"),
        f"{prefix}_planta": orig("planta"),
        f"{prefix}_puerta": orig("puerta"),
    })

    df = pd.concat([df, parsed], axis=1)

    norm = parsed.rename(columns=lambda c: c.replace(f"{prefix}_", "")).apply(
        normalize_parsed, axis=1
    )
    df[f"direccion_normalizada_{prefix}"] = norm
    return df






def main():
    bien = pd.read_excel('bienes.xlsx', dtype=str)
    titular = pd.read_excel('titulares.xlsx', dtype=str)
    lixo = pd.read_excel('lixo.xlsx', dtype=str)

    # ensure titulares has the full reference by borrowing the ctrl digits
    if 'id_fullref' not in titular.columns and {
        'id_parcela', 'numero_responsables'
    }.issubset(titular.columns) and {
        'id_parcela', 'numero_responsables', 'id_ctr1', 'id_ctr2', 'id_fullref'
    }.issubset(bien.columns):
        merge_cols = ['id_parcela', 'numero_responsables']
        tit_tmp = titular.merge(
            bien[merge_cols + ['id_ctr1', 'id_ctr2', 'id_fullref']],
            on=merge_cols,
            how='left'
        )
        titular = tit_tmp
        if 'id_fullref' not in titular.columns:
            titular['id_fullref'] = (
                titular['id_parcela']
                + titular['numero_responsables'].astype(int).astype(str).str.zfill(4)
                + titular['id_ctr1'].fillna('')
                + titular['id_ctr2'].fillna('')
            )

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


    #conflictos de dirección

    bien_conf = bien[['id_fullref', 'direccion_normalizada_bien']].drop_duplicates('id_fullref')
    tit_conf = titular[['id_fullref', 'direccion_normalizada_tit']].drop_duplicates('id_fullref')
    lixo_conf = lixo[['id_fullref', 'direccion_normalizada_lixo']].drop_duplicates('id_fullref')

    conflictos = bien_conf.merge(
        tit_conf,
        on='id_fullref',
        how='outer'
    ).merge(
        lixo_conf,
        on='id_fullref',
        how='outer'
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