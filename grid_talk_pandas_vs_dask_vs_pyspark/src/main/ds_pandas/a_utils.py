import os
from glob import glob

import pandas as pd

from src.main.a_utils.constants import LocalData


def read_csv(use_py_arrow: bool = False) -> pd.DataFrame:
    dfs = []
    paths = glob(os.path.join(LocalData.FLIGHTS_CSV, "*.csv"))
    if use_py_arrow:
        for path in paths:
            dfs.append(pd.read_csv(path, dtype_backend="pyarrow", engine="pyarrow"))
    else:
        for path in paths:
            dfs.append(pd.read_csv(path))
    return pd.concat(dfs)


def read_json(use_py_arrow: bool = False) -> pd.DataFrame:
    dfs = []
    paths = glob(os.path.join(LocalData.FLIGHTS_JSON, "*.json"))
    if use_py_arrow:
        for path in paths:
            dfs.append(pd.read_json(path, dtype_backend="pyarrow", engine="pyarrow", lines=True))
    else:
        for path in paths:
            dfs.append(pd.read_json(path, lines=True))
    return pd.concat(dfs)


def read_parquet(use_py_arrow: bool = False) -> pd.DataFrame:
    dfs = []
    paths = glob(os.path.join(LocalData.FLIGHTS_PARQUET, "*.parquet"))
    if use_py_arrow:
        for path in paths:
            dfs.append(pd.read_parquet(path, dtype_backend="pyarrow", engine="pyarrow"))
    else:
        for path in paths:
            dfs.append(pd.read_parquet(path))
    return pd.concat(dfs)