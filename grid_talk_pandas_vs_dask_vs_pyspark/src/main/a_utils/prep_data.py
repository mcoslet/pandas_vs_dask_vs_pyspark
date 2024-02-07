import argparse
import pandas as pd
from faker import Faker
import numpy as np
from src.main.a_utils.constants import LocalData

DATASET_GENERATIONS = ("full", "transactions", "product_details")
FILE_EXTENSION = ('json', 'csv', 'parquet')


def save_df(df: pd.DataFrame, file_name: str) -> None:
    print(f"Saving dataframe {df.shape} to {file_name}")
    file_path_to_save = destination / f"{file_name}.{file_extension}"
    if file_extension == "csv":
        df.to_csv(file_path_to_save, index=False)
    elif file_extension == "json":
        df.to_json(file_path_to_save, orient="records", lines=True)
    elif file_extension == "parquet":
        df.to_parquet(file_path_to_save)


def prep_synthetic_transactions(num_rows=500000) -> None:
    print("Calling prep_synthetic_transactions...")
    fake = Faker()
    rng = np.random.default_rng(seed=42)
    data = {
        "UserID": rng.integers(low=10000, high=100000, size=num_rows),
        "TransactionDate": np.array([fake.date_between(start_date='-2y', end_date='today') for _ in range(num_rows)]),
        "ProductID": rng.integers(1000, 10000, size=num_rows),
        "ProductName": rng.choice(
            ["Laptop", "Smartphone", "Tablet", "Headphones", "Camera", "Printer", "Monitor", "Keyboard", "Mouse",
             "Speaker"], size=num_rows),
        "Category": rng.choice(["Electronics", "Home & Office", "Wearables", "Photography", "Gaming"], size=num_rows),
        "Quantity": rng.integers(1, 11, size=num_rows),
        "UnitPrice": np.round(rng.uniform(10, 5000, size=num_rows), 2),
        "PaymentType": rng.choice(["Credit Card", "Debit Card", "PayPal", "Bank Transfer"], size=num_rows),
        "Country": np.array([fake.country() for _ in range(num_rows)])
    }
    df = pd.DataFrame(data)
    df["TotalPrice"] = df["Quantity"] * df["UnitPrice"]
    save_df(df, "transactions")


def prep_synthetic_product_details():
    print("Calling prep_synthetic_product_details...")
    fake = Faker()
    rng = np.random.default_rng(seed=42)
    unique_product_ids = np.arange(1000, 10000)

    data = {
        "ProductID": unique_product_ids,
        "SupplierName": [fake.company() for _ in unique_product_ids],
        "SupplierCountry": [fake.country() for _ in unique_product_ids],
        "ManufactureDate": [fake.date_between(start_date='-5y', end_date='today') for _ in unique_product_ids],
        "Weight": np.round(rng.uniform(0.1, 100.0, size=len(unique_product_ids)), 2),
        "Dimensions": [f"{rng.integers(1, 100)}x{rng.integers(1, 100)}x{rng.integers(1, 100)}" for _ in
                       unique_product_ids]
    }

    df = pd.DataFrame(data)
    save_df(df, "product_details")
    print("Product details dataset generated and saved.")


def prep_data() -> None:
    if dataset_to_create in ("full", "transactions"):
        prep_synthetic_transactions()
    if dataset_to_create in ("full", "product_details"):
        prep_synthetic_product_details()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='PrepData',
        description='Prepare synthetic data for DS DynamicTalk',
        epilog='Happy Coding :)')
    parser.add_argument("--dataset", choices=DATASET_GENERATIONS, help="Choose which dataset to create", default="full")
    parser.add_argument("--destination", help="Destination on where to save dataset", default=LocalData.DATA_DIR)
    parser.add_argument('--file_extension', choices=FILE_EXTENSION,
                        help='The file extension to save the dataset as.', default="csv")

    args = parser.parse_args()
    dataset_to_create = args.dataset
    destination = args.destination
    file_extension = args.file_extension
    prep_data()
