import os
from glob import glob

import pandas as pd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData


def read_csv(use_py_arrow: bool = False) -> pd.DataFrame:
    dfs = []
    paths = glob(os.path.join(LocalData.FLIGHTS_DIR, "*.csv"))
    if use_py_arrow:
        for path in paths:
            dfs.append(pd.read_csv(path, dtype_backend="pyarrow", engine="pyarrow"))
    else:
        for path in paths:
            dfs.append(pd.read_csv(path))
    return pd.concat(dfs)


@timeit_decorator()
def read_csv_with_pandas_pyarrow_engine():
    read_csv(use_py_arrow=True)


@timeit_decorator()
def read_csv_with_pandas_default_engine():
    read_csv(use_py_arrow=False)


def count_memory(df: pd.DataFrame):
    memory_per_column = df.memory_usage(deep=True)
    total_memory = round(memory_per_column.sum() / (2 ** 20))
    print(memory_per_column)
    print(f"Total memory usage: {total_memory} megabytes")


@timeit_decorator()
def filtering_pandas(df: pd.DataFrame):
    df[df["Category"] == "Gaming"]


if __name__ == "__main__":
    py_arrow_df = read_csv(True)
    default_df = read_csv()
    print(f"Performance Benchmark pandas for dataset with shape: {py_arrow_df.shape}")

    print("read_csv_with_pandas_pyarrow_engine: ")
    read_csv_with_pandas_pyarrow_engine()
    count_memory(py_arrow_df)

    print("read_csv_with_default_engine: ")
    read_csv_with_pandas_default_engine()
    count_memory(default_df)
