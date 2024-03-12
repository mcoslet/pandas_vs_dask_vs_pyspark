from pyspark.sql import DataFrame
from pyspark.sql.types import StructField, IntegerType, DoubleType, StringType, StructType

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.ds_pyspark.a_utils import read_csv


@timeit_decorator()
def easy_filtering_operation(df: DataFrame, action: bool = False):
    january_flights = df.where("Month = 1")
    if action:
        january_flights.count()


@timeit_decorator()
def heavy_filtering_operation(df: DataFrame, action: bool = False):
    df = df.select("UniqueCarrier", "ArrDelay")
    average_delay_by_carrier = df.groupby('UniqueCarrier').avg("ArrDelay")
    if action:
        average_delay_by_carrier.count()


if __name__ == "__main__":
    schema = StructType([
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("DayofMonth", IntegerType(), True),
        StructField("DayOfWeek", IntegerType(), True),
        StructField("DepTime", DoubleType(), True),
        StructField("CRSDepTime", IntegerType(), True),
        StructField("ArrTime", DoubleType(), True),
        StructField("CRSArrTime", IntegerType(), True),
        StructField("UniqueCarrier", StringType(), True),
        StructField("FlightNum", IntegerType(), True),
        StructField("TailNum", StringType(), True),
        StructField("ActualElapsedTime", DoubleType(), True),
        StructField("CRSElapsedTime", IntegerType(), True),
        StructField("AirTime", DoubleType(), True),
        StructField("ArrDelay", DoubleType(), True),
        StructField("DepDelay", DoubleType(), True),
        StructField("Origin", StringType(), True),
        StructField("Dest", StringType(), True),
        StructField("Distance", IntegerType(), True),
        StructField("TaxiIn", DoubleType(), True),
        StructField("TaxiOut", DoubleType(), True),
        StructField("Cancelled", IntegerType(), True),
        StructField("Diverted", IntegerType(), True)
    ])

    df = read_csv(infer_schema=True)
    schema_df = read_csv(schema=schema)
    print(f"Filtering Performance Benchmark PySpark for dataset with shape: {(df.count(), len(df.columns))}")

    print("easy_filtering_operation, action False:")
    easy_filtering_operation(df)

    print("easy_filtering_operation, action True:")
    easy_filtering_operation(df, action=True)

    print("heavy_filtering_operation, action False:")
    heavy_filtering_operation(df)

    print("heavy_filtering_operation, action True:")
    heavy_filtering_operation(df, action=True)

    print("easy_filtering_operation schema_df, action False:")
    easy_filtering_operation(schema_df)

    print("easy_filtering_operation schema_df, action True:")
    easy_filtering_operation(schema_df, action=True)

    print("heavy_filtering_operation schema_df, action False:")
    heavy_filtering_operation(schema_df)

    print("heavy_filtering_operation schema_df, action True:")
    heavy_filtering_operation(schema_df, action=True)
