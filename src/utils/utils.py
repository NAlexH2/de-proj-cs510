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


# I am not clever enough to do this. Between finding formulas online and asking
# ChatGPT a variety of questions on how to do it, this was the result. It's called
# the 'Haversine Formula' and it calculates distance between two points. In this
# case it returns a long/lat tuple to use.
# However, this is an approximation
# of the coordinates due to using only the distance in meters between the two
# points, and the origin (point_a) long/lat.
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
