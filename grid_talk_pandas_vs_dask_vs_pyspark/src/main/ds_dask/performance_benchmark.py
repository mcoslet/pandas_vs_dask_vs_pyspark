import dask.dataframe as dd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData


@timeit_decorator()
def read_csv_with_dask_pyarrow_engine():
    df = dd.read_csv(LocalData.DATA_DIR / "transactions.csv", dtype_backend="pyarrow", engine="pyarrow")
    # df.compute()


@timeit_decorator()
def read_csv_with_dask_default_engine():
    df = dd.read_csv(LocalData.DATA_DIR / "transactions.csv", dtype_backend="pyarrow", engine="pyarrow")
    # df.compute()


def count_memory():
    # df = dd.read_csv(LocalData.DATA_DIR / "transactions.csv", dtype_backend="pyarrow", engine="pyarrow")
    df = dd.read_csv(LocalData.DATA_DIR / "transactions.csv")
    memory_per_column = df.memory_usage(deep=True)
    total_memory = round(memory_per_column.compute().sum() / (2 ** 20))
    print(memory_per_column.compute())
    print(f"Total memory usage: {total_memory} megabytes")


read_csv_with_dask_pyarrow_engine()
count_memory()
