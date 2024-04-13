import os

from datetime import datetime
import shutil
from src.grabber import DataGrabber
from src.uploadgdrive import write_to_gdrive
from src.vars import API_URL, DATA_FOLDER, DATA_MONTH_DAY, DATA_PATH


if __name__ == "__main__":
    data_collect = DataGrabber()
    data_collect.data_grabber()
    # write_to_gdrive()

    # shutil.rmtree(DATA_FOLDER)
