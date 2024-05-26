import logging, os
from datetime import datetime

# Utilities used throughout

# The bus breadcrumb data use in main.py
BREADCRUMB_API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

# Stop ID API url used in pipe3.py
STOPID_API_URL = "https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num="

# Get the current systems month and day. This program doesn't enforce any tz
# restrictions so ensure the VM is in the tz you want before running.
DATA_MONTH_DAY = datetime.now().strftime("%m-%d")

# main.py data path for the breadcrumb data rcvd to be stored to
RAW_DATA_FOLDER = os.path.join("raw_data_files")
RAW_DATA_PATH = os.path.join(RAW_DATA_FOLDER, DATA_MONTH_DAY)

# pipe3.py data path for the stop id data rcvd to be stored to
STOPID_DATA_FOLDER = os.path.join("stop_id_data")
STOPID_DATA_PATH = os.path.join(STOPID_DATA_FOLDER, DATA_MONTH_DAY)

# Where the breadcrumb subscriber saves its data to
BC_SUBSCRIBER_FOLDER = os.path.join("bc_subscriber_data_files")
BC_SUBSCRIBER_DATA_PATH_JSON = os.path.join(
    BC_SUBSCRIBER_FOLDER, f"{DATA_MONTH_DAY}.json"
)

# Where the stopid subscriber saves its data to.
SID_SUBSCRIBER_FOLDER = os.path.join("sid_subscriber_data_files")
SID_SUBSCRIBER_DATA_PATH_JSON = os.path.join(
    SID_SUBSCRIBER_FOLDER, f"{DATA_MONTH_DAY}.json"
)


# Function used in logging with specific format
def curr_time_micro() -> str:
    return f"[{datetime.now().strftime('%m-%d-%Y-%H:%M:%S.%f')[:-3]}]"


# Function to get month-day-year used in many places throughout the code
def mdy_string() -> str:
    return datetime.now().strftime("%m-%d-%Y")


# Log and print function
def log_and_print(message: str, prend: str = "\n") -> None:
    """Will log and print a message

    Arguments:
        message Specifies what the message to log/print will be

        prend Specifies if the print will or won't have a end= arg to be used.
        Defaults to \n

    Returns:
        None
    """
    logging.info(msg=f"{curr_time_micro()} {message}")
    print(f"{curr_time_micro()} {message}", end=prend)
