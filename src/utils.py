import os
from datetime import datetime

# Utilities used throughout
API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_FOLDER = os.path.join("raw_data_files")
DATA_MONTH_DAY = datetime.now().strftime("%m-%d")
FULL_DATA_PATH = os.path.join(DATA_FOLDER, DATA_MONTH_DAY)
SUBSCRIBER_FOLDER = os.path.join("subscriber_data_files")
SUBSCRIBER_DATA_PATH_JSON = os.path.join(
    SUBSCRIBER_FOLDER, f"{DATA_MONTH_DAY}.json"
)


def curr_time_micro() -> str:
    return f"[{datetime.now().strftime('%m-%d-%Y-%H:%M:%S.%f')[:-3]}]"


def mdy_time() -> str:
    return datetime.now().strftime("%m-%d-%Y")
