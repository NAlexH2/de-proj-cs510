import math
import logging, os, sys
from datetime import datetime

# Utilities used throughout
BREADCRUMB_API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
STOPID_API_URL = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="
DATA_MONTH_DAY = datetime.now().strftime("%m-%d")
RAW_DATA_FOLDER = os.path.join("raw_data_files")
RAW_DATA_PATH = os.path.join(RAW_DATA_FOLDER, DATA_MONTH_DAY)

STOPID_DATA_FOLDER = os.path.join("stop_id_data")
STOPID_DATA_PATH = os.path.join(STOPID_DATA_FOLDER, DATA_MONTH_DAY)

SUBSCRIBER_FOLDER = os.path.join("subscriber_data_files")
SUBSCRIBER_DATA_PATH_JSON = os.path.join(
    SUBSCRIBER_FOLDER, f"{DATA_MONTH_DAY}.json"
)


def curr_time_micro() -> str:
    return f"[{datetime.now().strftime('%m-%d-%Y-%H:%M:%S.%f')[:-3]}]"


def mdy_string() -> str:
    return datetime.now().strftime("%m-%d-%Y")


def log_and_print(message: str, prend: str = "\n") -> None:
    """Will log and print a message

    Arguments:
        message Specifies what the message to log/print will be

        prend Specifies if the print will or won't have a end= arg to be used.

    Returns:
        None
    """
    logging.info(msg=f"{curr_time_micro()} {message}")
    print(f"{curr_time_micro()} {message}", end=prend)


def sub_logger(message: str, prend: str = "\n") -> None:
    logging.info(msg=f"{curr_time_micro()} {message}")
    print(f"{curr_time_micro()} {message}", end=prend)
