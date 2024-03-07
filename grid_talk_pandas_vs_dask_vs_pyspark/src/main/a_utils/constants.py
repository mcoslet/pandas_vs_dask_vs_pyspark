import os
from pathlib import Path


class LocalData:
    SRC_DIR = Path(__file__).parent.parent.parent
    RESOURCES_DIR = SRC_DIR / 'resources/'
    DATA_DIR = RESOURCES_DIR / 'data/'
    FLIGHTS_CSV = DATA_DIR / 'nycflights'
    FLIGHTS_JSON = DATA_DIR / 'flightjson'
    FLIGHTS_PARQUET = DATA_DIR / 'flightparquet'
    SYNTHETIC_CSV = DATA_DIR / 'synthetic'

