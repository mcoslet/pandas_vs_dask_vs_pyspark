from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType, DoubleType

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData

spark = SparkSession.builder.appName("Grid Dynamic Talk").master("local[*]").getOrCreate()


def read_csv(infer_schema: bool = False, schema=None) -> DataFrame:
    if infer_schema:
        return spark.read.csv(str(LocalData.FLIGHTS_DIR / "*.csv"), header=True)
    if schema is not None:
        return spark.read.csv(str(LocalData.FLIGHTS_DIR / "*.csv"), header=True, schema=schema)
    raise ValueError("No schema provided")


@timeit_decorator()
def read_csv_with_spark_inferschema_true():
    read_csv(infer_schema=True)


@timeit_decorator()
def read_csv_with_spark_inferschema_false(schema):
    read_csv(schema=schema)


def count_memory(df: DataFrame):
    df = df.cache().select(df.columns)
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df.unpersist(blocking=True)
    print(size_in_bytes / (2 ** 20))


@timeit_decorator()
def filter_spark(df: DataFrame):
    df.filter(df['Category'] == "Gaming")


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
    df = read_csv(schema=schema)
    print(f"Performance Benchmark PySpark for dataset with shape: {(df.count(), len(df.columns))}")

    print("read_csv_with_spark_inferschema_true: ")
    read_csv_with_spark_inferschema_true()

    print("read_csv_with_spark_inferschema_false: ")
    read_csv_with_spark_inferschema_false(schema=schema)

    print("Memory count: ")
    count_memory(df)
