import hashlib
import os.path
import tempfile

import requests

from services.utils.constants import FilePaths


def check_md5_for_pdf(pdf_url):
    response = requests.get(pdf_url)
    with tempfile.TemporaryDirectory() as tmp:
        file_name = os.path.join(tmp, "orar.pdf")
        with open(file_name, 'wb') as f:
            f.write(response.content)
            return _check_md5(file_name)


def _check_md5(file_name):
    with open(file_name, 'rb') as file_to_check:
        data = file_to_check.read()
        return hashlib.md5(data).hexdigest()


def _is_schedule_updated(new_md5):
    first_year_old_md5 = open(FilePaths.FIRST_YEAR_FILE_PATH).readline()
    return new_md5 != first_year_old_md5


def check_if_schedule_is_updated(pdf_url):
    md5 = check_md5_for_pdf(pdf_url)
    if _is_schedule_updated(md5):
        with open(FilePaths.FIRST_YEAR_FILE_PATH, mode="w+") as f:
            old_md5 = f.readline()
            f.write(md5)
        print(f"The scheduler is updated, new md5: {md5}, old md5: {old_md5}")
    else:
        print("The scheduler is the same. Keep calm and drink coffe")