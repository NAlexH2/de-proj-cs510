import os, sys, tarfile
from pathlib import Path

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    DATA_MONTH_DAY,
    FULL_DATA_PATH,
    log_and_print,
)


def tar_data():
    log_and_print(message=f"Starting tar for {DATA_MONTH_DAY}.")
    file_exists = os.path.exists(f"{FULL_DATA_PATH}.tar")
    if file_exists:
        os.remove(f"{FULL_DATA_PATH}.tar")
    tar = tarfile.open(f"{FULL_DATA_PATH}.tar", "w")
    files = os.listdir(f"{FULL_DATA_PATH}")
    files.sort()
    for file in files:
        file_path = os.path.join(FULL_DATA_PATH, file)
        tar.add(file_path, arcname=file_path)
        log_and_print(message=f"{file} added to tar.")
    tar.close()
    log_and_print(message=f"tar for {DATA_MONTH_DAY} complete.")
