import dask.dataframe as dd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.ds_dask.a_utils import read_csv


@timeit_decorator()
def easy_filtering_operation(df: dd.DataFrame, compute=False):
    january_flights = df[df['Month'] == 1]
    if compute:
        january_flights.compute()


@timeit_decorator()
def heavy_filtering_operation(df: dd.DataFrame, compute=False):
    df = df[['UniqueCarrier', 'ArrDelay']]
    average_delay_by_carrier = df.groupby('UniqueCarrier')['ArrDelay'].mean().reset_index()
    if compute:
        average_delay_by_carrier.compute()


if __name__ == "__main__":
    py_arrow_df = read_csv(True)
    default_df = read_csv()
    print(f"Filtering Performance Benchmark Pandas for dataset with shape: {py_arrow_df.compute().shape}")

    print("easy_filtering_operation with pyarrow, compute False: ")
    easy_filtering_operation(py_arrow_df)

    print("easy_filtering_operation with pyarrow, compute True: ")
    easy_filtering_operation(py_arrow_df, compute=True)

    print("easy_filtering_operation without pyarrow, compute False: ")
    easy_filtering_operation(default_df)

    print("easy_filtering_operation without pyarrow, compute True: ")
    easy_filtering_operation(default_df, compute=True)

    print("heavy_filtering_operation with pyarrow, compute False: ")
    heavy_filtering_operation(py_arrow_df)

    print("heavy_filtering_operation with pyarrow, compute True: ")
    heavy_filtering_operation(py_arrow_df, compute=True)

    print("heavy_filtering_operation without, compute False: ")
    heavy_filtering_operation(default_df)

    print("heavy_filtering_operation without, compute True: ")
    heavy_filtering_operation(default_df, compute=True)
