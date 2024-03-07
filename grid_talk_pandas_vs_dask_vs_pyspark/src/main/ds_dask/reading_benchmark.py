from src.main.a_utils.benchmark_function_time import timeit_decorator
import dask.dataframe as dd

from src.main.ds_dask.a_utils import read_csv, read_json, read_parquet


@timeit_decorator()
def read_csv_with_dask_pyarrow_engine(compute=False):
    read_csv(use_py_arrow=True, compute=compute)


@timeit_decorator()
def read_json_with_dask_pyarrow_engine(compute=False):
    read_json(use_py_arrow=True, compute=compute)


@timeit_decorator()
def read_parquet_with_dask_pyarrow_engine(compute=False):
    read_parquet(use_py_arrow=True, compute=compute)


@timeit_decorator()
def read_csv_with_dask_default_engine(compute=False):
    read_csv(use_py_arrow=False, compute=compute)


@timeit_decorator()
def read_json_with_dask_engine(compute=False):
    read_json(use_py_arrow=False, compute=compute)


@timeit_decorator()
def read_parquet_with_dask_default_engine(compute=False):
    read_parquet(use_py_arrow=False, compute=compute)


@timeit_decorator()
def read_parquet_with_dask_pyarrow_engine(compute=False):
    read_parquet(use_py_arrow=True, compute=compute)


def count_memory(df: dd.DataFrame):
    print("Count memory: ")
    memory_per_column = df.memory_usage(deep=True)
    total_memory = round(memory_per_column.compute().sum() / (2 ** 20))
    print(memory_per_column.compute())
    print(f"Total memory usage: {total_memory} megabytes")


if __name__ == "__main__":
    py_arrow_csv_df = read_csv(True)
    default_csv_df = read_csv()

    py_arrow_json_df = read_json(True)
    default_json_df = read_json()

    py_arrow_parquet_df = read_parquet(True)
    default_parquet_df = read_parquet()
    print(f"Reading Performance Benchmark Dask for dataset with shape: {py_arrow_csv_df.compute().shape}")

    print("read_csv_with_dask_pyarrow_engine: ")
    read_csv_with_dask_pyarrow_engine()
    count_memory(py_arrow_csv_df)

    print("read_csv_with_dask_default_engine: ")
    read_csv_with_dask_default_engine()
    count_memory(default_csv_df)

    print("read_csv_with_dask_pyarrow_engine, compute True: ")
    read_csv_with_dask_pyarrow_engine(compute=True)

    print("read_csv_with_dask_default_engine, compute True: ")
    read_csv_with_dask_default_engine(compute=True)

    print("read_json_with_pyarrow_engine: ")
    read_json_with_dask_pyarrow_engine()
    count_memory(py_arrow_json_df)

    print("read_json_with_dask_default_engine: ")
    read_json_with_dask_engine()
    count_memory(default_json_df)

    print("read_json_with_pyarrow_engine, compute True: ")
    read_json_with_dask_pyarrow_engine(compute=True)

    print("read_json_with_dask_default_engine, compute True: ")
    read_json_with_dask_engine(compute=True)

    print("read_parquet_with_dask_pyarrow_engine: ")
    read_parquet_with_dask_pyarrow_engine()
    count_memory(py_arrow_parquet_df)

    print("read_parquet_with_dask_default_engine: ")
    read_parquet_with_dask_default_engine()
    count_memory(default_parquet_df)

    print("read_parquet_with_dask_pyarrow_engine, compute True: ")
    read_parquet_with_dask_pyarrow_engine(compute=True)

    print("read_parquet_with_dask_default_engine, compute True: ")
    read_parquet_with_dask_default_engine(compute=True)

