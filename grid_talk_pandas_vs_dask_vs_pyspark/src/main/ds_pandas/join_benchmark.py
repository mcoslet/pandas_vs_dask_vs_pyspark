import pandas as pd

from src.main.a_utils.benchmark_function_time import timeit_decorator
from src.main.a_utils.constants import LocalData
from src.main.ds_pandas.a_utils import read_csv


@timeit_decorator()
def default_join(first_df: pd.DataFrame, second_df: pd.DataFrame) -> pd.DataFrame:
    return pd.merge(first_df, second_df, how="inner", left_index=True, right_index=True)


if __name__ == "__main__":
    df = read_csv(use_py_arrow=True).set_index("Dest")
    second_df = pd.read_csv(LocalData.SYNTHETIC_CSV / "synthetic.csv", dtype_backend="pyarrow", engine="pyarrow").set_index("Dest")
    df_sample = df.sample(frac=0.15, random_state=42)
    print(f"Join Performance Benchmark Pandas for dataset with shape: {df_sample.shape} and {second_df.shape}")
    default_join(df_sample, second_df)
