import os.path
from pathlib import Path


class FcimUrls:
    SCHEDULE_URL = "https://fcim.utm.md/procesul-de-studii/orar/#toggle-id-3"


class FilePaths:
    _MAIN_PATH = Path(__file__).parent.parent.parent
    _RESOURCES_PATH = _MAIN_PATH / "resources"
    FIRST_YEAR_FILE_PATH = _RESOURCES_PATH / "first_year.txt"
    SECOND_YEAR_FILE_PATH = _RESOURCES_PATH / "second_year.txt"
