import os.path

import dask.dataframe as dd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData


def read_csv(use_py_arrow: bool = False, compute=False) -> dd.DataFrame:
    path = os.path.join(LocalData.FLIGHTS_DIR, "*.csv")
    if use_py_arrow:
        df = dd.read_csv(LocalData.FLIGHTS_DIR / "*.csv", dtype_backend="pyarrow", engine="pyarrow",
                         dtype={'AirTime': 'double[pyarrow]',
                                'CRSElapsedTime': 'double[pyarrow]',
                                'TailNum': 'string[pyarrow]',
                                'TaxiIn': 'int64[pyarrow]',
                                'TaxiOut': 'int64[pyarrow]'})
    else:
        df = dd.read_csv(path, dtype={'CRSElapsedTime': 'float64',
                                      'TailNum': 'object'})
    if compute:
        df.compute()
    return df


@timeit_decorator()
def read_csv_with_dask_pyarrow_engine(compute=False):
    read_csv(use_py_arrow=True, compute=compute)


@timeit_decorator()
def read_csv_with_dask_default_engine(compute=False):
    read_csv(use_py_arrow=False, compute=compute)


def count_memory(df: dd.DataFrame):
    memory_per_column = df.memory_usage(deep=True)
    total_memory = round(memory_per_column.compute().sum() / (2 ** 20))
    print(memory_per_column.compute())
    print(f"Total memory usage: {total_memory} megabytes")


@timeit_decorator()
def filtering_dask(df: dd.DataFrame):
    df[df["Category"] == "Gaming"].compute()


if __name__ == "__main__":
    py_arrow_df = read_csv(True)
    default_df = read_csv()
    print(f"Performance Benchmark pandas for dataset with shape: {py_arrow_df.compute().shape}")

    print("read_csv_with_dask_pyarrow_engine: ")
    read_csv_with_dask_pyarrow_engine()
    count_memory(py_arrow_df)

    print("read_csv_with_dask_default_engine: ")
    read_csv_with_dask_default_engine()
    count_memory(default_df)

    print("read_csv_with_dask_pyarrow_engine, compute True: ")
    read_csv_with_dask_pyarrow_engine(compute=True)

    print("read_csv_with_dask_default_engine, compute True: ")
    read_csv_with_dask_default_engine(compute=True)
    # filtering_dask(df)
