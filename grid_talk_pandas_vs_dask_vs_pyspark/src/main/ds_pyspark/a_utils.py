from pyspark.sql import DataFrame, SparkSession

from src.main.a_utils.constants import LocalData

spark = (SparkSession
         .builder
         .appName("Grid Dynamic Talk")
         .master("local[*]")
         .config("spark.driver.memory", "10g")
         .getOrCreate())


def read_csv(infer_schema: bool = False, schema=None) -> DataFrame:
    if infer_schema:
        return spark.read.csv(str(LocalData.FLIGHTS_CSV / "*.csv"), header=True)
    if schema is not None:
        return spark.read.csv(str(LocalData.FLIGHTS_CSV / "*.csv"), header=True, schema=schema)
    raise ValueError("No schema provided")


def read_json(infer_schema: bool = False, schema=None) -> DataFrame:
    if infer_schema:
        return spark.read.json(str(LocalData.FLIGHTS_JSON / "*.json"))
    if schema is not None:
        return spark.read.json(str(LocalData.FLIGHTS_JSON / "*.json"), schema=schema)
    raise ValueError("No schema provided")


def read_parquet(infer_schema: bool = False, schema=None) -> DataFrame:
    if infer_schema:
        return spark.read.parquet(str(LocalData.FLIGHTS_PARQUET / "*.parquet"), header=True)
    if schema is not None:
        return spark.read.parquet(str(LocalData.FLIGHTS_PARQUET / "*.parquet"), header=True, schema=schema)
