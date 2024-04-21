import os
import shutil
import sys

from src.dataemailer import tar_data
from src.grabber import DataGrabber
from src.uploadgdrive import upload_to_gdrive
from src.publisher import publish_data
from src.utils import curr_time_micro


if __name__ == "__main__":
    if "-U" in sys.argv:
        print(
            f"{curr_time_micro()} Upload arg found. Will send to google drive "
            + "nharris@pdx.edu."
        )
    if "-P" in sys.argv:
        print(
            f"{curr_time_micro()} Publish arg found. Will push ALL entries in "
            + "EVERY FILE to Google pub/sub."
        )
    data_collect = DataGrabber()
    data_collect.data_grabber()
    OK_size = data_collect.OK_response.size
    bad_size = data_collect.bad_response.size

    # TODO: gmail acc to email from to myself
    # data_emailer(ok_size=OK_size, bad_size=bad_size)

    tar_data()  # Just tar the file instead. For now.

    if "-U" in sys.argv:
        start_time = curr_time_micro()
        upload_to_gdrive()
        print(
            f"{curr_time_micro()} Upload to google drive completed. "
            + f"Started at {start_time}."
        )

    if "-P" in sys.argv:
        start_time = curr_time_micro()
        publish_data()  # Publish to pub/sub
        print(f"{curr_time_micro()} Publish complete. Started at {start_time}.")

    # shutil.rmtree(FULL_DATA_PATH)
    print(f"{curr_time_micro()} Operation finished. Cleanup complete.")
