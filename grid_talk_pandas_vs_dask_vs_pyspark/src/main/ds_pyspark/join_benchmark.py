from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData
from src.main.ds_pyspark.a_utils import read_csv, spark


@timeit_decorator()
def default_join(first_df: DataFrame, second_df: DataFrame) -> DataFrame:
    return first_df.join(second_df, on="Year", how="inner")


@timeit_decorator()
def broadcast_join(first_df: DataFrame, second_df: DataFrame) -> DataFrame:
    return first_df.join(broadcast(second_df), on="Year", how="inner")


if __name__ == "__main__":
    df = read_csv(infer_schema=True)
    second_df = spark.read.csv(str(LocalData.SYNTHETIC_CSV / "synthetic.csv"), header=True, inferSchema=True)
    print(
        f"Filtering Performance Benchmark PySpark for dataset with shape: {(df.count(), len(df.columns))} "
        f"and {(second_df.count(), len(second_df.columns))}"
    )

    print("default_join: ")
    default_join(df, second_df)

    print("broadcast_join: ")
    broadcast_join(df, second_df)
