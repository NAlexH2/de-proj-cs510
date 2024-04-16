import os
import shutil
import sys

from datetime import datetime
from src.dataemailer import data_emailer
from src.grabber import DataGrabber
from src.uploadgdrive import upload_to_gdrive


if __name__ == "__main__":
    if "-U" in sys.argv:
        print("\nUpload arg found. Will send to google drive nharris@pdx.edu.")

    data_collect = DataGrabber()
    data_collect.data_grabber()
    OK_size = data_collect.OK_response.size
    bad_size = data_collect.bad_response.size
    # data_emailer(ok_size=OK_size, bad_size=bad_size)

    if "-U" in sys.argv:
        upload_to_gdrive()
        print("\nUpload to google drive completed.")

    # shutil.rmtree(DATA_FOLDER)
