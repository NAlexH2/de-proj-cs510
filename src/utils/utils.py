import math
import logging, os, sys
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


def log_or_print(
    message: str, use_print: bool = False, prend: str = "\n"
) -> None:
    """Either uses the built in logging library or prints based on sys.argv
    containing -L option

    Arguments:
        message Specifies what the message to log/print will be

        use_print Specifies to use print for the message. If -L is present in
        sys.argv, this will log and print. Otherwise, will just print.

        prend Specifies if the print will or won't have a end= arg to be used.

    Returns:
        None
    """
    if "-L" in sys.argv and not use_print:
        logging.info(msg=message)
    elif "-L" in sys.argv and use_print:
        logging.info(msg=message)
        print(message, end=prend)
    else:
        print(message, end=prend)


def sub_logger(message: str, prend: str = "\n") -> None:
    logging.info(msg=message)
    print(message, end=prend)


def lat_long_filler(point_a, aDistance, bDistance):
    R = 6371  # Radius of the Earth in kilometers

    distance_traveled = abs(bDistance - aDistance)

    # Convert distance from meters to kilometers
    distance_traveled_km = distance_traveled / 1000

    # Calculate change in latitude and longitude
    delta_lat = math.degrees(distance_traveled_km / R)
    delta_lon = delta_lat / math.cos(math.radians(point_a[1]))

    # Calculate coordinates of point B
    point_b = (point_a[0] + delta_lon, point_a[1] + delta_lat)

    return point_b
