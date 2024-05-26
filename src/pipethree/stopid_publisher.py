from pathlib import Path
import logging, os, sys, json
from google.oauth2 import service_account
from concurrent import futures
from google.cloud import pubsub_v1

# If this file is ran from within its own directory it lives in
# (./src/mainpipe) ensure the base project directory is included in the scripts
# execution path.
if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    log_and_print,
    STOPID_DATA_PATH,
    DATA_MONTH_DAY,
)


SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
TOPIC_ID = "StopData"


# This script is capable of publishing the current days data as long as the data
# actually exists for this day, and is able to do this independent of the main
# path from pipethree.py
class PipelinePublisherSID:
    def __init__(self):
        # Establish authorized creds
        self.pubsub_creds = (
            service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE
            )
        )

        # make the publisher from the creds
        self.publisher = pubsub_v1.PublisherClient(
            credentials=self.pubsub_creds
        )

        # set the topic path to publish to using the setup
        self.topic_path = self.publisher.topic_path(PROJECT_ID, TOPIC_ID)

        # The following var is getting information from the pipethree.py main
        # track to later be published. When this file is ran independently,
        # the file is loaded locally in to this var to then be published
        # outside of the normal track.
        self.data_to_publish: list[str] = []

        # How many records have been published this cycle. Excellent for
        # tracking stats to ensure it's accurately publishing all our data.
        self.total_records = 0

    def add_to_publish_list(self, data):
        """Used in the main track of pipethree.py if -P command line arg is present.
        Adds data to the list to be published and stores it memory for super
        fast publishing.

        Arguments:
            data -- Data from the DataGrabberBC class when pipethree.py is ran.
        """
        # These are only actively used when pipethree.py is ran.
        self.data_to_publish.append(data)
        self.total_records += len(json.loads(data))
        return

    def futures_callback(self, future: futures.Future):
        """Callback used for every publisher

        Arguments:
            future -- Result from the publish that waits to be acknowledge for
            the result to ensure it is published correctly. This is a required
            step to ensure all the messages are published
        """
        try:
            future.result()  # Wait for the message to be properly resulted (?)
        except Exception as e:
            # Log and handle the exception appropriately
            logging.error(f"Error in future: {e}")

    def publish_data(self):
        record_count = 0  # Metric to report
        future = None  # last future caught

        # List of futures waiting for results from. This has a blocking for
        # loop to wait until they are all properly popped from this list.
        # This ensures all messages are published before moving on. This
        # is ***A*** solution, and it's because not all future.results()
        # happen quick enough and threads get piled up and may be lost when the
        # script ends.
        futures_list = []

        logging.info("\n")
        log_and_print(f"Publishing all records to {TOPIC_ID}.")
        while len(self.data_to_publish) > 0:
            to_publish = self.data_to_publish.pop()
            to_publish_json = json.loads(to_publish)

            for record in to_publish_json:
                encoded_record = json.dumps(record).encode("utf-8")
                future: futures.Future = self.publisher.publish(
                    self.topic_path, data=encoded_record
                )
                record_count += 1

                future.add_done_callback(self.futures_callback)
                futures_list.append(future)

        log_and_print(f"")
        log_and_print(f"Waiting on Publisher futures...\n")

        # Blocking for loop to ensure all results are properly completed.
        for future in futures.as_completed(futures_list):
            if future.cancelled():
                log_and_print(f"{future.exception()}")

        log_and_print(
            message=f"Publishing complete. Total records published: {record_count}.\n"
        )

        return


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/SID-PUB-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    pub_worker = PipelinePublisherSID()

    # This follows a common pattern of accessing the current days directory,
    # publishing all the files in that folder for that day (as long as the
    # folder exists) and then publishes them.

    # This only happens in this specific way if and only if this script is
    # ran by itself. Otherwise it follows the pipethree.py path.
    files_list = []
    if os.path.exists(STOPID_DATA_PATH):
        files_list = os.listdir(STOPID_DATA_PATH)
        files_list.sort()

        log_and_print(
            message=f"Stop ID publisher starting with directory {STOPID_DATA_PATH}."
        )
        while len(files_list) > 0:
            file_to_open = files_list.pop()
            curr_file = open(os.path.join(STOPID_DATA_PATH, file_to_open))
            json_from_file = json.load(curr_file)
            pub_worker.add_to_publish_list(json.dumps(json_from_file))

        pub_worker.publish_data()
    else:
        log_and_print(
            message=f"Folder {STOPID_DATA_PATH} does not exist. Quitting publishing."
        )
        sys.exit(0)
    log_and_print(f"Stop ID publisher finished.")
    sys.exit(0)
