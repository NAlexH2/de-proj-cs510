import os
import sys
import logging
import time

from src.tardata import tar_data
from src.grabber import DataGrabber
from src.uploadgdrive import upload_to_gdrive
from src.utils import DATA_MONTH_DAY, curr_time_micro, log_or_print
from src.publisher import PipelinePublisher


def found_args():
    started_by_bash = False
    if len(sys.argv) == 1 and "-G" not in sys.argv:
        print(
            """
MISSING NECESSARY ARGS

Usage: python main.py -G [OPTIONS]
  -G\tGather all assigned vehicles from src/vehicle_ids.csv.
    \tSaves all data in 'raw_data_files' directory.
  -B\tDesignate that a bash script was used -- Assumes you are doing redirection
    \twith the bash script.
  -L\tEnables logging to a file. Shares the same file as found in
    \tgo_main.sh.
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

    if "-L" in sys.argv:
        os.makedirs("logs", exist_ok=True)
        logging.basicConfig(
            format="",
            filename=f"logs/MAINLOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.INFO,
        )
        message = (
            f"\n{curr_time_micro()} Logging arg found. Will store any "
            + f"data output data to logs/MAINLOG-{DATA_MONTH_DAY}.log"
        )
        print(f"{message}")
        log_or_print(message=message)

    if "-G" in sys.argv:
        msg = (
            f"{curr_time_micro()} Gather arg found. Will pull all assigned "
            + "vehicles from PSU API."
        )
        log_or_print(message=msg)

    if "-U" in sys.argv:
        msg = (
            f"{curr_time_micro()} Upload arg found. Will send to google drive "
            + "nharris@pdx.edu."
        )
        log_or_print(message=msg)

    if "-T" in sys.argv:
        msg = f"{curr_time_micro()} Tar arg found. Will tarball each record."
        log_or_print(message=msg)

    if "-P" in sys.argv:
        msg = (
            f"{curr_time_micro()} Publish arg found. Will push ALL entries "
            + "in EVERY RECORD to Google pub/sub."
        )
        log_or_print(message=msg)


if __name__ == "__main__":
    found_args()
    if "-G" in sys.argv:
        pub_worker: PipelinePublisher = PipelinePublisher()
        data_collect = DataGrabber(pub_worker=pub_worker)
        data_collect.data_grabber_main()
        OK_size = data_collect.OK_response.size
        bad_size = data_collect.bad_response.size

        # TODO: gmail acc to email from to myself
        # data_emailer(ok_size=OK_size, bad_size=bad_size)

        if "-U" in sys.argv:
            start_time = curr_time_micro()
            upload_to_gdrive()
            log_or_print(
                message=f"{curr_time_micro()} Upload to google drive completed. "
                + f"Started at {start_time}."
            )

        if "-T" in sys.argv:
            tar_data()  # Just tar the file instead. For now.

        # Publish all the data that's been collected so far.
        if "-P" in sys.argv:
            start_time = curr_time_micro()
            pub_worker.publish_data()
            log_or_print(
                message=f"{curr_time_micro()} Publish started at {start_time}.",
                use_print=True,
                prend="\n",
            )

        log_or_print(
            message=f"\n{curr_time_micro()} Operation finished.",
            use_print=True,
            prend="\n",
        )
    sys.exit()
