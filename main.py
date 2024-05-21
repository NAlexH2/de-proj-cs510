import os
import sys
import logging
import time
import traceback

from src.mainpipe.tardata import tar_data
from mainpipe.breadcrumb_grabber import DataGrabber
from src.mainpipe.uploadgdrive import upload_to_gdrive
from mainpipe.breadcrumb_publisher import PipelinePublisher
from src.utils.utils import DATA_MONTH_DAY, curr_time_micro, log_and_print


def found_args():
    started_by_bash = False
    if len(sys.argv) == 1 and "-G" not in sys.argv:
        print(
            """
MISSING NECESSARY ARGS

Usage: python main.py -G [OPTIONS]
  -G\tGather all assigned vehicles from src/vehicle_ids.csv.
    \tSaves all data in 'raw_data_files' directory.
  -U\tUploads to drive shared with by service account and keys in
    \t'data_eng_key' directory.
  -T\tTars all data pulled from PSU Bus API.
  -P\tPublishes all data to google Pub/Sub using service account
    \tand keys provided by the user in 'data_eng_key' directory.
            """
        )
        print("Exiting program...")
        time.sleep(1)
        sys.exit()

    if "-G" in sys.argv:
        msg = "Gather arg found. Will pull all assigned vehicles from PSU API."
        log_and_print(message=msg)

    if "-U" in sys.argv:
        msg = "Upload arg found. Will send to google drive nharris@pdx.edu."
        log_and_print(message=msg)

    if "-T" in sys.argv:
        msg = "Tar arg found. Will tarball each record."
        log_and_print(message=msg)

    if "-P" in sys.argv:
        msg = "Publish arg found. Will push ALL entries in EVERY RECORD to Google pub/sub."
        log_and_print(message=msg)


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/MAINLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    try:
        found_args()
        if "-G" in sys.argv:
            pub_worker: PipelinePublisher = PipelinePublisher()
            data_collect = DataGrabber(pub_worker=pub_worker)
            data_collect.data_grabber_main()
            OK_size = data_collect.OK_response.size
            bad_size = data_collect.bad_response.size

            if "-U" in sys.argv:
                start_time = curr_time_micro()
                upload_to_gdrive()
                log_and_print(
                    message="Upload to google drive completed. "
                    + f"Started at {start_time}."
                )

            if "-T" in sys.argv:
                logging.info("\n")
                tar_data()
                logging.info("\n")

            # Publish all the data that's been collected so far.
            if "-P" in sys.argv:
                start_time = curr_time_micro()
                pub_worker.publish_data()
                log_and_print(message=f"Publish started at {start_time}.")

            log_and_print(message="\nOperation finished.")
    except Exception as e:
        logging.basicConfig(
            format="",
            filename=f"logs/MAINLOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.ERROR,
        )
        log_and_print(message="EXCEPTION THROWN!")
        log_and_print(message=f"Traceback:\n{traceback.format_exc()}")
        log_and_print(message=f"Exception as e:\n{e}")

    sys.exit()
