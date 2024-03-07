import os
import dask.dataframe as dd
from src.main.a_utils.constants import LocalData

dd_dtype_schema = {'AirTime': 'double[pyarrow]',
                   'CRSElapsedTime': 'double[pyarrow]',
                   'TailNum': 'string[pyarrow]',
                   'TaxiIn': 'int64[pyarrow]',
                   'TaxiOut': 'int64[pyarrow]'}


def read_csv(use_py_arrow: bool = False, compute=False) -> dd.DataFrame:
    path = os.path.join(LocalData.FLIGHTS_CSV, "*.csv")
    if use_py_arrow:
        df = dd.read_csv(path, dtype_backend="pyarrow", engine="pyarrow",
                         dtype=dd_dtype_schema)
    else:
        df = dd.read_csv(path, dtype={'CRSElapsedTime': 'float64',
                                      'TailNum': 'object'})
    if compute:
        df.compute()
    return df


def read_json(use_py_arrow: bool = False, compute=False) -> dd.DataFrame:
    path = os.path.join(LocalData.FLIGHTS_JSON, "*.json")
    if use_py_arrow:
        df = dd.read_json(path, dtype={'CRSElapsedTime': 'float64',
                                       'TailNum': 'object'}, lines=True)
    else:
        df = dd.read_json(path, dtype={'CRSElapsedTime': 'float64',
                                       'TailNum': 'object'})
    if compute:
        df.compute()
    return df


def read_parquet(use_py_arrow: bool = False, compute=False) -> dd.DataFrame:
    path = os.path.join(LocalData.FLIGHTS_PARQUET, "*.parquet")
    if use_py_arrow:
        df = dd.read_parquet(path, dtype_backend="pyarrow", engine="pyarrow",
                             dtype=dd_dtype_schema)
    else:
        df = dd.read_parquet(path, dtype={'CRSElapsedTime': 'float64',
                                          'TailNum': 'object'})
    if compute:
        df.compute()
    return df
