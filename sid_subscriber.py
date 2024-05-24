import logging
import json, os
import time
import traceback
from src.pipethree.stopid_store import SIDDataToSQLDB

from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from src.utils.utils import (
    DATA_MONTH_DAY,
    SID_SUBSCRIBER_DATA_PATH_JSON,
    SID_SUBSCRIBER_FOLDER,
    curr_time_micro,
    log_and_print,
)

# Where are the important keys
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"  # What project owns the subscription
SID_SUB_ID = "StopDataRcvr"  # What's the subscription name to pair onto
TIMEOUT = 600  # Set the timeout time for the subscriber to suspend listening


# This script functions as a service on a linux based machine. It will listen
# until the machine shuts down. Works great in both 24/7 and dynamic situations.
class PipelineSubscriberStopData:
    """Creates a PipeLineSubscriber based on the
    data provided in the global consts in the file"""

    def __init__(self) -> None:
        # Using this version to access the account key generated and downloaded
        # from GCP stored in data_eng_key
        self.pubsub_creds = (
            service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE
            )
        )

        # This is the breadcrumb topic subscriber using the authorized generated
        # creds from above. This establishes permissions and scopes for this
        # subscriber and what it is allowed to do.
        self.sid_subscriber = pubsub_v1.SubscriberClient(
            credentials=self.pubsub_creds
        )

        # Define the subscription path to use. Created when this script starts
        # up as a service in the name-guard at the bottom of this file.
        self.sid_sub_path = self.sid_subscriber.subscription_path(
            PROJECT_ID, SID_SUB_ID
        )

        # Data storage in memory to work with throughout the subscriber
        self.data_to_write: list[str] = []

        # Tracking number to keep an eye on how many records we've received
        # to line up if an error occurs or to monitor with.
        self.current_listener_records = 0

    def store_to_sql(self, jData: list[dict]) -> None:
        """Stores data  to a specified database

        Arguments:
            jData -- json data from the BreadCrumbRcvr subscription
        """
        log_and_print(f"Sending stop ID data to SQL trip table.")
        db_worker = SIDDataToSQLDB(jData)
        db_worker.to_db_start()
        log_and_print(f"Stop ID data update to SQL trip table complete!")

    def write_records_to_file(self):
        """Writes the data located in memory to a file respective to the current
        data in memory with self.data_to_write

        """
        json_data: list[dict] = []
        log_and_print("")
        log_and_print(f"Total records: {self.current_listener_records}")

        # Pop each record into a single var then append the json conversion to
        # json_data.
        while len(self.data_to_write) > 0:
            data_prep = self.data_to_write.pop()
            json_data.append(json.loads(data_prep))

        log_and_print(f"Writing all records to a single file.")
        log_and_print(f"{SID_SUBSCRIBER_DATA_PATH_JSON}")

        # Make the missing dir if it's missing
        if not os.path.exists(SID_SUBSCRIBER_FOLDER):
            os.makedirs(SID_SUBSCRIBER_FOLDER)

        # Write to the file regardless if there's one already there.
        # This should only ever run once. We do not care about any existing data.
        with open(SID_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
            json.dump(json_data, outfile, indent=4)

        # Absolutely must wait for breadcrumbs to be written to the SQL database
        # before proceeding.
        log_and_print(
            f"{SID_SUB_ID} subscriber sleeping for 30 minutes to wait for "
            + "BreadCrumbRcvr to finish."
        )
        time.sleep(1800)

        # Where the db storage magic happens!
        self.store_to_sql(json_data)

        return

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        """Function that handles any response from the Pub/Sub
        subscription being accessed. This stores the response in memory to be
        used later in the pipeline.

        Arguments:
            message -- The data received from subscription.
        """
        rcvd_data: bytes = message.data

        # Decode the bytes from utf-8 to parsable string
        decoded_data = rcvd_data.decode()

        # Store it in memory.
        self.data_to_write.append(decoded_data)
        self.current_listener_records = len(self.data_to_write)

        # Give ourselves some message to ensure the script is still running
        if self.current_listener_records % 10 == 0:
            log_and_print(
                message=f"Approximate records received so "
                + f"far: {self.current_listener_records}",
                prend="\r",
            )

        # Ack the message to clear it from the subscription so that we do not
        # ever receive it again.
        message.ack()
        return

    def subscriber_listener(self):
        """Actively uses the current subscriber to sit and wait for messages
        from the subscription assigned to it.
        """
        log_and_print(f"Subscriber actively listening...")
        streaming_future = self.sid_subscriber.subscribe(
            self.sid_sub_path, callback=self.callback
        )
        with self.sid_subscriber:
            try:
                streaming_future.result(timeout=TIMEOUT)
                # Timeout is when to stop listening, so that the rest of the
                # script can take place.
            except TimeoutError:
                streaming_future.cancel()
                streaming_future.result()
                return


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/SID-SUBLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )

    # Must wait some time due to how the pipeline functions across the
    # environments the each interface with.
    log_and_print(
        message=f"Subscriber sleeping for 20 minutes to allow publisher to publish first.\n"
    )
    time.sleep(1200)

    try:  # This process is delicate, we want to know exactly why it failed if
        # it fails. It shouldn't, but it could.
        sub_worker = PipelineSubscriberStopData()
        start_time = curr_time_micro()
        total_records_overall = 0

        log_and_print(
            message=f"{start_time} Subscriber {SID_SUB_ID} starting.\n"
        )

        while True:  # Run until the machine shuts down.

            # Kick on the listener function, runs for as long as the timeout
            # parameter has been set
            sub_worker.subscriber_listener()

            # If we received some data in the last round of timeouts, write it
            # to file and then store it in the SQL database
            if sub_worker.current_listener_records > 0:
                sub_worker.write_records_to_file()
                log_and_print(
                    message=f"Subscriber started at {start_time}. "
                    + f" Subscriber complete."
                )

                # Continue to keep track of the total records worked on
                total_records_overall += sub_worker.current_listener_records
                sub_worker.current_listener_records = 0

            else:
                # If there was no data received, just let the system log it
                # and be known by the owner that it ceased receiving messages
                log_and_print(
                    message=f"No data received in the past {TIMEOUT//60} minutes.\n"
                )
                total_records_overall += sub_worker.current_listener_records
                sub_worker.current_listener_records = 0

            log_and_print(
                message=f"Have received and saved "
                + f"{total_records_overall} records up to this point."
            )
            log_and_print(
                message=f"Subscriber re-starting to continue "
                + f"listening for messages."
            )

            # Required to create a new subscriber as the previous one had been
            # closed. We must do this each loop.
            sub_worker.sid_subscriber = pubsub_v1.SubscriberClient(
                credentials=sub_worker.pubsub_creds
            )
            start_time = curr_time_micro()
    except Exception as e:  # Oops something went wrong.
        logging.basicConfig(
            format="",
            filename=f"logs/SID-SUBLOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.ERROR,
        )
        log_and_print(f"EXCEPTION THROWN!")
        log_and_print(f"Traceback:\n{traceback.format_exc()}")
        log_and_print(f"Exception as e:\n{e}")
