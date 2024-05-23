from pathlib import Path
import logging, os, sys, json
from google.oauth2 import service_account
from concurrent import futures
from google.cloud import pubsub_v1

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


class PipelinePublisher:
    def __init__(self):
        self.pubsub_creds = (
            service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE
            )
        )
        self.publisher = pubsub_v1.PublisherClient(
            credentials=self.pubsub_creds
        )
        self.topic_path = self.publisher.topic_path(PROJECT_ID, TOPIC_ID)
        self.data_to_publish: list[str] = []
        self.total_records = 0

    def add_to_publish_list(self, data):
        self.data_to_publish.append(data)
        self.total_records += len(json.loads(data))
        return

    def futures_callback(self, future: futures.Future):
        try:
            future.result()
        except Exception as e:
            # Log or handle the exception appropriately
            logging.error(f"Error in future: {e}")

    def publish_data(self):
        record_count = 0
        future = None
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
    pub_worker = PipelinePublisher()
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
