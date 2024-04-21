import os
import shutil
import sys

from src.dataemailer import tar_data
from src.grabber import DataGrabber
from src.uploadgdrive import upload_to_gdrive
from src.utils import FULL_DATA_PATH


# TODO: Each output message should have the same DTG format as the bash script
# TODO: gmail acc to email from to myself

if __name__ == "__main__":
    if "-U" in sys.argv:
        print("\nUpload arg found. Will send to google drive nharris@pdx.edu.")

    data_collect = DataGrabber()
    data_collect.data_grabber()
    OK_size = data_collect.OK_response.size
    bad_size = data_collect.bad_response.size

    # TODO See if you can find another soln to get the emailer working.
    # data_emailer(ok_size=OK_size, bad_size=bad_size)

    tar_data()  # Just tar the file instead. For now.

    if "-U" in sys.argv:
        upload_to_gdrive()
        print("\nUpload to google drive completed.")

    shutil.rmtree(FULL_DATA_PATH)
    print("\nOperation finished. Cleanup complete.")
