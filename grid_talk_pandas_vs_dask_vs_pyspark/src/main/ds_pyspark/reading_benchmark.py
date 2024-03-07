from pandas import DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType, DoubleType

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.ds_pyspark.a_utils import read_csv, read_json, read_parquet


@timeit_decorator()
def read_csv_with_spark_inferschema_true():
    read_csv(infer_schema=True)


@timeit_decorator()
def read_csv_with_spark_inferschema_false(schema: StructType):
    read_csv(schema=schema)


@timeit_decorator()
def read_json_with_spark_inferschema_true():
    read_json()


@timeit_decorator()
def read_json_with_spark_inferschema_false(schema: StructType):
    read_json(schema=schema)


@timeit_decorator()
def read_parquet_with_spark_inferschema_true():
    read_parquet()


@timeit_decorator()
def read_parquet_with_spark_inferschema_false(schema: StructType):
    read_parquet(schema=schema)


@timeit_decorator()
def count_memory(df: DataFrame):
    print("Count memory: ")
    df = df.cache().select(df.columns)
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df.unpersist(blocking=True)
    print(size_in_bytes / (2 ** 20))


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
        StructField("Distance", DoubleType(), True),
        StructField("TaxiIn", DoubleType(), True),
        StructField("TaxiOut", DoubleType(), True),
        StructField("Cancelled", IntegerType(), True),
        StructField("Diverted", IntegerType(), True)
    ])

    df_csv_infer_schema = read_csv(infer_schema=True)
    df_csv_with_schema = read_csv(schema=schema)
    df_json_infer_schema = read_json(infer_schema=True)
    df_json_with_schema = read_json(schema=schema)
    df_parquet_infer_schema = read_parquet(infer_schema=True)
    df_parquet_with_schema = read_parquet(schema=schema)

    print(f"Reading Performance Benchmark PySpark for dataset with shape: {(df_csv_infer_schema.count(), len(df_csv_infer_schema.columns))}")

    print("read_csv_with_spark_inferschema_true: ")
    read_csv_with_spark_inferschema_true()
    count_memory(df_csv_infer_schema)

    print("read_csv_with_spark_inferschema_false: ")
    read_csv_with_spark_inferschema_false(schema=schema)
    count_memory(df_csv_with_schema)

    print("read_json_with_spark_inferschema_true: ")
    read_json_with_spark_inferschema_false()
    count_memory(df_json_infer_schema)

    print("read_json_with_spark_inferschema_false: ")
    read_json_with_spark_inferschema_false(schema=schema)
    count_memory(df_json_with_schema)

    print("read_parquet_with_spark_inferschema_true: ")
    read_parquet_with_spark_inferschema_true()
    count_memory(df_parquet_infer_schema)

    print("read_parquet_with_spark_inferschema_false: ")
    read_parquet_with_spark_inferschema_false(schema=schema)
    count_memory(df_parquet_with_schema)
