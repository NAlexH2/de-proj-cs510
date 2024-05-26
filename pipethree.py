import os
import sys
import logging
import time
import traceback

from src.pipethree.stopid_grabber import DataGrabberSID
from src.pipethree.stopid_publisher import PipelinePublisherSID
from src.utils.utils import DATA_MONTH_DAY, curr_time_micro, log_and_print


# This script is used to commence the gathering of the third pipe in the op.
# This is the stop id data main script which collects the necessary and missing
# data from the first pipe in main.py. Stop ID data gives day of week,
# direction, and the bus route the vehicle was working for that trip id.


# -G is required, the rest are optional.
def found_args():
    """Alerts the user to what args were found or if it's missing -G then
    provides a usage statement.
    """
    if len(sys.argv) == 1 and "-G" not in sys.argv:
        print(
            """
MISSING NECESSARY ARGS

Usage: python main.py -G [OPTIONS]
  -G\tGather all assigned vehicles from src/vehicle_ids.csv.
    \tSaves all data in 'stop_id_data' directory as .json files.
  -P\tPublishes all data to google Pub/Sub using service account
    \tand keys provided by the user in 'data_eng_key' directory.
            """
        )
        print("Exiting program...\n")
        time.sleep(1)
        sys.exit()

    if "-G" in sys.argv:
        msg = (
            f"Gather arg found. Will pull all assigned "
            + "vehicles from Stop ID API and save them as .json files."
        )
        log_and_print(msg)

    if "-P" in sys.argv:
        msg = (
            f"Publish arg found. Will push ALL entries "
            + "in EVERY RECORD to Google pub/sub."
        )
        log_and_print(msg)


if __name__ == "__main__":
    # Immediately want to make sure the logs folder exists for all the info
    # being logged.
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/PIPETHREELOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    try:
        found_args()  # Inform the user of what ops will take place
        if "-G" in sys.argv:  # Required arg
            pub_worker: PipelinePublisherSID = PipelinePublisherSID()
            data_collect = DataGrabberSID(pub_worker=pub_worker)
            data_collect.data_grabber_main()

            # Publish all the data that's been collected so far.
            if "-P" in sys.argv:
                start_time = curr_time_micro()
                pub_worker.publish_data()
                log_and_print(f"Publish started at {start_time}.")

            log_and_print(f"Operation finished.")
    except Exception as e:
        logging.basicConfig(
            format="",
            filename=f"logs/PIPETHREELOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.ERROR,
        )
        logging.info("\n")
        log_and_print(f"EXCEPTION THROWN!")
        log_and_print(f"Traceback:\n{traceback.format_exc()}\n")
        log_and_print(f"Exception as e:\n{e}")

    sys.exit()
