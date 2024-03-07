from pyspark.sql import DataFrame

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.ds_pyspark.a_utils import read_csv


@timeit_decorator()
def easy_filtering_operation(df: DataFrame):
    january_flights = df.where("Month = 1")


@timeit_decorator()
def heavy_filtering_operation(df: DataFrame):
    df = df.select("UniqueCarrier", "ArrDelay")
    average_delay_by_carrier = df.groupby('UniqueCarrier').avg("ArrDelay")


if __name__ == "__main__":
    df = read_csv(infer_schema=True)
    print(f"Filtering Performance Benchmark PySpark for dataset with shape: {(df.count(), len(df.columns))}")

    print("easy_filtering_operation:")
    easy_filtering_operation(df)

    print("heavy_filtering_operation:")
    heavy_filtering_operation(df)
