import os
import shutil

from datetime import datetime
from src.grabber import DataGrabber
from src.uploadgdrive import upload_to_gdrive


if __name__ == "__main__":
    data_collect = DataGrabber()
    data_collect.data_grabber()
    upload_to_gdrive()

    # shutil.rmtree(DATA_FOLDER)
