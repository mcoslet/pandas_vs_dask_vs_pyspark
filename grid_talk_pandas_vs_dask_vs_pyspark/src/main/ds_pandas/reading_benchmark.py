import pandas as pd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.ds_pandas.a_utils import read_csv, read_json, read_parquet


@timeit_decorator()
def read_csv_with_pandas_pyarrow_engine():
    read_csv(use_py_arrow=True)


@timeit_decorator()
def read_csv_with_pandas_default_engine():
    read_csv(use_py_arrow=False)


@timeit_decorator()
def read_json_with_pandas_pyarrow_engine():
    read_json(use_py_arrow=True)


@timeit_decorator()
def read_json_with_pandas_default_engine():
    read_json()


@timeit_decorator()
def read_parquet_with_pandas_pyarrow_engine():
    read_parquet(use_py_arrow=True)


@timeit_decorator()
def read_parquet_with_pandas_default_engine():
    read_parquet()


def count_memory(df: pd.DataFrame):
    memory_per_column = df.memory_usage(deep=True)
    total_memory = round(memory_per_column.sum() / (2 ** 20))
    print(memory_per_column)
    print(f"Total memory usage: {total_memory} megabytes")


if __name__ == "__main__":
    py_arrow_csv_df = read_csv(True)
    default_csv_df = read_csv()

    py_arrow_json_df = read_json(True)
    default_json_df = read_json()

    py_parquet_parquet_df = read_parquet(True)
    default_py_parquet_df = read_parquet()

    print(f"Reading Performance Benchmark Pandas for dataset with shape: {py_arrow_csv_df.shape}")
    print(default_csv_df.Origin.value_counts().to_string())
    print("read_csv_with_pandas_pyarrow_engine: ")
    read_csv_with_pandas_pyarrow_engine()
    count_memory(py_arrow_csv_df)

    print("read_csv_with_default_engine: ")
    read_csv_with_pandas_default_engine()
    count_memory(default_csv_df)

    print("read_json_with_pandas_pyarrow_engine: ")
    read_json_with_pandas_pyarrow_engine()
    count_memory(py_arrow_json_df)

    print("read_json_with_pandas_default_engine: ")
    read_json_with_pandas_default_engine()
    count_memory(default_json_df)

    print("read_parquet_with_pandas_pyarrow_engine: ")
    read_parquet_with_pandas_pyarrow_engine()
    count_memory(py_parquet_parquet_df)

    print("read_parquet_with_default_engine: ")
    read_parquet_with_pandas_default_engine()
    count_memory(default_py_parquet_df)