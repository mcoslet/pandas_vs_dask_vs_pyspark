import time
import sys
import argparse
import os
from glob import glob

import tarfile
import urllib.request

from numpy.random import default_rng
import pandas as pd
import dask.array as da
from faker import Faker

from src.main.a_utils.constants import LocalData

DATASETS = ["synthetic", "flights", "all"]
FILE_EXTENSIONS = ["csv", "json", "parquet"]
data_dir = LocalData.DATA_DIR


def parse_args(args=None):
    parser = argparse.ArgumentParser(
        description="Downloads, generates and prepares data for the Grid Dynamic Talk"
    )
    parser.add_argument(
        "--small",
        action="store_true",
        default=None,
        help="Whether to use smaller example datasets. Checks SMALL_DF environment variable if not specified.",
    )
    parser.add_argument(
        "-d", "--dataset", choices=DATASETS, help="Datasets to generate.", default="all"
    )
    parser.add_argument(
        "-fe", "--file_extension",
        choices=FILE_EXTENSIONS,
        help="File extension to save the dataset",
        default="csv"
    )

    return parser.parse_args(args)


if not os.path.exists(data_dir):
    raise OSError(
        "data/ directory not found, aborting data preparation. "
        'Restore it with "git checkout data" from the base '
        "directory."
    )


def flights(small=None):
    start = time.time()
    flights_raw = os.path.join(data_dir, "nycflights.tar.gz")
    flightdir = os.path.join(data_dir, "nycflights")
    jsondir = os.path.join(data_dir, "flightjson")
    parquetdir = os.path.join(data_dir, "flightparquet")
    pandas_dtype_schema = {
        'AirTime': 'float64',
        'CRSElapsedTime': 'float64',
        'TailNum': 'string',
        'TaxiIn': 'Int64',
        'TaxiOut': 'Int64',
        'Distance': 'Int64'
    }
    if small is None:
        small = bool(os.environ.get("SMALL_DF", False))

    if small:
        n = 500
    else:
        n = 10_000

    if not os.path.exists(flights_raw):
        print("- Downloading NYC Flights dataset... ", end="", flush=True)
        url = "https://storage.googleapis.com/dask-tutorial-data/nycflights.tar.gz"
        urllib.request.urlretrieve(url, flights_raw)
        print("done", flush=True)

    if not os.path.exists(flightdir):
        print("- Extracting flight data... ", end="", flush=True)
        tar_path = os.path.join(data_dir, "nycflights.tar.gz")
        with tarfile.open(tar_path, mode="r:gz") as flights:
            flights.extractall(data_dir)

        if small:
            for path in glob(os.path.join(data_dir, "nycflights", "*.csv")):
                with open(path, "r") as f:
                    lines = f.readlines()[:1000]

                with open(path, "w") as f:
                    f.writelines(lines)

        print("done", flush=True)

    if not os.path.exists(jsondir):
        print("- Creating json data... ", end="", flush=True)
        os.mkdir(jsondir)
        for path in glob(os.path.join(data_dir, "nycflights", "*.csv")):
            prefix = os.path.splitext(os.path.basename(path))[0]
            df = pd.read_csv(path, nrows=n)
            df = df.astype(pandas_dtype_schema)
            df.to_json(
                os.path.join(jsondir, prefix + ".json"),
                orient="records",
                lines=True,
            )
        print("done", flush=True)

    if not os.path.exists(parquetdir):
        print("- Creating parquet data... ", end="", flush=True)
        os.mkdir(parquetdir)
        for path in glob(os.path.join(data_dir, "nycflights", "*.csv")):
            prefix = os.path.splitext(os.path.basename(path))[0]
            df = pd.read_csv(path, nrows=n)
            df = df.astype(pandas_dtype_schema)
            df.to_parquet(
                os.path.join(parquetdir, prefix + ".parquet")
            )
        print("done", flush=True)
    else:
        return

    end = time.time()
    print("** Created flights dataset! in {:0.2f}s**".format(end - start))


def create_synthetic_dataset(small):
    start = time.time()
    if small:
        size = 2_000
    else:
        size = 20_000
    Faker.seed(42)
    rng = default_rng(seed=42)
    fake = Faker(["en_US"])
    dest_airports = ['ORD', 'BOS', 'ATL', 'LAX', 'MIA', 'DFW', 'DCA', 'MCO', 'DTW', 'PIT',
                     'SFO', 'FLL', 'CLT', 'DEN', 'CLE', 'STL', 'PBI', 'IAH', 'TPA', 'MSP',
                     'SJU', 'BUF', 'CMH', 'CVG', 'RDU', 'GSO', 'ORF', 'PHX', 'BWI', 'ROC',
                     'IND', 'RIC', 'IAD', 'BNA', 'MEM', 'MSY', 'SYR', 'LAS', 'MCI', 'RSW',
                     'JAX', 'SEA', 'DAY', 'SLC', 'PWM', 'SAN', 'SRQ', 'PVD', 'MDW', 'PHL',
                     'BTV', 'STT', 'GSP', 'HOU', 'BDL', 'SDF', 'DAB', 'CHS', 'PDX', 'MLB',
                     'SNA', 'MKE', 'MHT', 'CAE', 'SAT', 'COS', 'BQN', 'SJC', 'ALB', 'AUS',
                     'SAV', 'OMA', 'EGE', 'HNL', 'BGR', 'MYR', 'LWB', 'ORH', 'TYS', 'ROA',
                     'HDN', 'PSE', 'CRW', 'ACK', 'BHM', 'ABE', 'ANC', 'CHO', 'SWF', 'MTJ',
                     'EWR', 'ICT', 'ISP', 'JFK']
    synthetic_data = {
        'Year': rng.integers(1990, 2000, size),
        'UniqueCarrier': [fake.bothify(text='??', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(size)],
        'AircraftRegistrationId': [fake.bothify(text='??-####', letters='ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in
                                   range(size)],
        'FlightNum': rng.integers(1, 2000, size),
        'Distance': rng.integers(50, 5000, size),
        'Origin': [fake.bothify(text='???').upper() for _ in range(size)],
        'Dest': rng.choice(dest_airports, size)

    }
    if not LocalData.SYNTHETIC_CSV.exists():
        os.mkdir(LocalData.SYNTHETIC_CSV)
    pd.DataFrame(synthetic_data).set_index('Year').to_csv(LocalData.SYNTHETIC_CSV / 'synthetic.csv')
    end = time.time()
    print("** Created synthetic dataset! in {:0.2f}s**".format(end - start))


def main(args=None):
    args = parse_args(args)
    if args.dataset == "synthetic" or args.dataset == "all":
        print("Creating synthetic dataset")
        create_synthetic_dataset(args.small)
    if args.dataset == "flights" or args.dataset == "all":
        print("Creating nyc flights dataset")
        flights(args.small)


if __name__ == "__main__":
    sys.exit(main())
