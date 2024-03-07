import dask.dataframe as dd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.ds_dask.a_utils import read_csv


@timeit_decorator()
def light_filtering_operation(df: dd.DataFrame):
    january_flights = df[df['Month'] == 1]


@timeit_decorator()
def heavy_filtering_operation(df: dd.DataFrame):
    df = df[['UniqueCarrier', 'ArrDelay']]
    average_delay_by_carrier = df.groupby('UniqueCarrier')['ArrDelay'].mean().reset_index().compute()


if __name__ == "__main__":
    py_arrow_df = read_csv(True)
    default_df = read_csv()
    print(f"Filtering Performance Benchmark Pandas for dataset with shape: {py_arrow_df.compute().shape}")

    print("easy_filtering_operation with pyarrow: ")
    light_filtering_operation(py_arrow_df)

    print("easy_filtering_operation without pyarrow: ")
    light_filtering_operation(default_df)

    print("heavy_filtering_operation with pyarrow: ")
    heavy_filtering_operation(py_arrow_df)

    print("heavy_filtering_operation without pyarrow: ")
    heavy_filtering_operation(default_df)
