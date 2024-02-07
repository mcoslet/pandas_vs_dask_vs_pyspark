from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, FloatType

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData

spark = SparkSession.builder.appName("Grid Dynamic Talk").master("local[*]").getOrCreate()


@timeit_decorator()
def read_csv_with_spark_inferschema_true():
    spark.read.csv(str(LocalData.DATA_DIR / "transactions.csv"), header=True)


@timeit_decorator()
def read_csv_with_spark_inferschema_false():
    schema = StructType([
        StructField("UserID", IntegerType(), True),
        StructField("TransactionDate", DateType(), True),
        StructField("ProductID", IntegerType(), True),
        StructField("ProductName", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Quantity", IntegerType(), True),
        StructField("UnitPrice", FloatType(), True),
        StructField("PaymentType", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("TotalPrice", FloatType(), True)
    ])
    spark.read.csv(str(LocalData.DATA_DIR / "transactions.csv"), header=True, schema=schema)


def count_memory():
    df = spark.read.csv(str(LocalData.DATA_DIR / "transactions.csv"), header=True)
    df = df.cache().select(df.columns)
    size_in_bytes = df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()
    df.unpersist(blocking=True)
    print(size_in_bytes / (2 ** 20))


count_memory()
