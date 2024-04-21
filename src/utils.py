from datetime import datetime
import os

# Utilities used throughout
API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_FOLDER = os.path.join("raw_data_files")
DATA_MONTH_DAY = datetime.now().strftime("%m-%d")
FULL_DATA_PATH = os.path.join(DATA_FOLDER, DATA_MONTH_DAY)
SUBSCRIBER_DATA_PATH = os.path.join("subscriber_data_files")
CURR_TIME_MICRO = (lambda dt: dt.strftime("%m-%d-%Y-%H:%M:%S.%f")[:-3])(
    datetime.now()
)
MDY_TIME = (lambda dt: dt.strftime("%m-%d-%Y"))(datetime.now())
