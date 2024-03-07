from src.main.a_utils.benchmark_function_time import timeit_decorator
import dask.dataframe as dd

from src.main.a_utils.constants import LocalData
from src.main.ds_dask.a_utils import read_csv


@timeit_decorator()
def default_join(first_df: dd.DataFrame, second_df: dd.DataFrame) -> dd.DataFrame:
    return first_df.merge(second_df, how="inner", left_index=True, right_index=True)


@timeit_decorator()
def broadcast_join(first_df: dd.DataFrame, second_df: dd.DataFrame) -> dd.DataFrame:
    return dd.merge(first_df, second_df, left_index=True, right_index=True, how="inner", broadcast=True)


if __name__ == "__main__":
    default_df = read_csv().set_index("Year")
    second_df = dd.read_csv(LocalData.SYNTHETIC_CSV / "synthetic.csv").set_index("Year")

    print(f"Join Performance Benchmark Pandas for dataset with shape: {default_df.shape} and {second_df.shape}")

    print("Default Join Performance Benchmark")
    default_join(default_df, second_df)
    print("Broadcast Join Performance")
    broadcast_join(default_df, second_df)
