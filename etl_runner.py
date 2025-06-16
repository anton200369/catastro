import argparse
import importlib
import pandas as pd
import yaml
from pathlib import Path


def load_dataset(name, ds_cfg, cols_cfg, cfg_dir):
    ds_dir = Path(ds_cfg['dir'])
    if not ds_dir.is_absolute():
        ds_dir = (cfg_dir / ds_dir).resolve()
    pattern = ds_cfg.get('pattern', '*')
    files = sorted(ds_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files for dataset {name} in {ds_dir} matching {pattern}")
    frames = [pd.read_excel(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    # select and rename columns
    mapping = {orig: info['as'] for orig, info in cols_cfg.items()}
    df = df[list(mapping.keys())].rename(columns=mapping)
    return df


def apply_builders(df, builders):
    for builder in builders.values():
        using = builder.get('using')
        if not using:
            continue
        mod_name, func_name = using.rsplit('.', 1)
        try:
            module = importlib.import_module(mod_name)
            func = getattr(module, func_name)
        except Exception:
            print(f"Builder {using} not available; skipping")
            continue
        params = builder.get('params', {})
        apply = builder.get('apply', 'frame')
        if apply == 'frame':
            df = func(df, **params)
        else:
            df[apply] = df[apply].apply(lambda x: func(x, **params))
    return df


def main(cfg_path):
    cfg_path = Path(cfg_path)
    cfg = yaml.safe_load(cfg_path.read_text())
    cfg_dir = cfg_path.parent

    datasets_cfg = cfg.get('datasets', {})
    cols_cfg = cfg.get('columns', {})
    data = {}
    for name, ds_cfg in datasets_cfg.items():
        data[name] = load_dataset(name, ds_cfg, cols_cfg.get(name, {}), cfg_dir)

    join_cfg = cfg.get('join', {})
    left = data[join_cfg['left']]
    right = data[join_cfg['right']]
    join_kwargs = {
        'how': join_cfg.get('how', 'left'),
        'on': join_cfg.get('on') or join_cfg.get('"on"'),
    }
    if 'validate' in join_cfg:
        join_kwargs['validate'] = join_cfg['validate']
    result = left.merge(right, **join_kwargs)

    builders_cfg = cfg.get('builders', {})
    if builders_cfg:
        result = apply_builders(result, builders_cfg)

    out_dir = Path(cfg['output']['dir'])
    if not out_dir.is_absolute():
        out_dir = (cfg_dir / out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / cfg['output']['name']
    result.to_csv(out_file, index=False)
    print(f"Written {len(result)} rows to {out_file}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run ETL based on YAML config')
    parser.add_argument('config', help='Path to YAML configuration file')
    args = parser.parse_args()
    main(args.config)
