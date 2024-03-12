from pyspark.sql import DataFrame, SparkSession

from src.main.a_utils.constants import LocalData

spark = (SparkSession
         .builder
         .appName("Grid Dynamic Talk")
         .master("local[*]")
         .config("spark.driver.memory", "10g")
         .getOrCreate())


def read_csv(infer_schema: bool = False, schema=None, action: bool = False) -> DataFrame:
    df = None
    if infer_schema:
        df = spark.read.csv(str(LocalData.FLIGHTS_CSV / "*.csv"), header=True)
    if schema is not None:
        df = spark.read.csv(str(LocalData.FLIGHTS_CSV / "*.csv"), header=True, schema=schema)
    if action and df is not None:
        df.count()
    if df is None:
        raise ValueError("No schema provided")
    return df


def read_json(infer_schema: bool = False, schema=None, action: bool = False) -> DataFrame:
    df = None
    if infer_schema:
        df = spark.read.json(str(LocalData.FLIGHTS_JSON / "*.json"))
    if schema is not None:
        df = spark.read.json(str(LocalData.FLIGHTS_JSON / "*.json"), schema=schema)
    if action and df is not None:
        df.count()
    if df is None:
        raise ValueError("No schema provided")
    return df


def read_parquet(infer_schema: bool = False, schema=None, action: bool = False) -> DataFrame:
    df = None
    if infer_schema:
        df = spark.read.parquet(str(LocalData.FLIGHTS_PARQUET / "*.parquet"), header=True)
    if schema is not None:
        df = spark.read.parquet(str(LocalData.FLIGHTS_PARQUET / "*.parquet"), header=True, schema=schema)
    if action and df is not None:
        df.count()
    if df is None:
        raise ValueError("No schema provided")
    return df
