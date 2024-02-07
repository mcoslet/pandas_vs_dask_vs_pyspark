import pandas as pd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData


@timeit_decorator()
def read_csv_with_pandas_pyarrow_engine():
    pd.read_csv(LocalData.DATA_DIR / "transactions.csv", dtype_backend="pyarrow", engine="pyarrow")


@timeit_decorator()
def read_csv_with_pandas_default_engine():
    pd.read_csv(LocalData.DATA_DIR / "transactions.csv")


def count_memory():
    df = pd.read_csv(LocalData.DATA_DIR / "transactions.csv", dtype_backend="pyarrow", engine="pyarrow")
    memory_per_column = df.memory_usage(deep=True)
    total_memory = round(memory_per_column.sum() / (2**20))
    print(memory_per_column)
    print(f"Total memory usage: {total_memory} megabytes")


read_csv_with_pandas_pyarrow_engine()
