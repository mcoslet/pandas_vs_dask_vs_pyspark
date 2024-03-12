from src.main.a_utils.benchmark_function_time import timeit_decorator
import dask.dataframe as dd

from src.main.a_utils.constants import LocalData
from src.main.ds_dask.a_utils import read_csv


@timeit_decorator()
def default_join(first_df: dd.DataFrame, second_df: dd.DataFrame, compute: bool = False) -> dd.DataFrame:
    result = dd.merge(first_df, second_df, how="inner", left_index=True, right_index=True)
    print(f"HERE: {result.npartitions}")
    if compute:
        result.compute()
    return result


@timeit_decorator()
def broadcast_join(first_df: dd.DataFrame, second_df: dd.DataFrame, compute: bool = False) -> dd.DataFrame:
    result = dd.merge(first_df, second_df, left_index=True, right_index=True, how="inner", broadcast=True)
    print(f"HEREBROAD: {result.npartitions}")
    if compute:
        result.compute()
    return result


if __name__ == "__main__":
    df = read_csv(use_py_arrow=True).set_index("Dest")
    second_df = dd.read_csv(LocalData.SYNTHETIC_CSV / "synthetic.csv", dtype_backend="pyarrow",
                            engine="pyarrow").set_index("Dest")
    df_sample = df.sample(frac=0.15, random_state=42)
    print(f"Join Performance Benchmark Pandas for dataset with shape: {df_sample.compute().shape} and {second_df.compute().shape}")

    print("Default Join Performance Benchmark")
    default_join(df, second_df)

    print("Broadcast Join Performance")
    broadcast_join(df, second_df)

    print("Default Join Performance Benchmark, compute=True")
    default_join(df, second_df, compute=True)

    print("Broadcast Join Performance, compute=True")
    broadcast_join(df, second_df, compute=True)
