import json
import os
import pandas as pd
from google.oauth2 import service_account
from google.cloud import pubsub_v1, storage
from concurrent.futures import TimeoutError

from utils.utils import SUBSCRIBER_DATA_PATH_JSON, SUBSCRIBER_FOLDER

SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
SUB_ID = "archivetest-sub"
TIMEOUT = 300

pubsub_creds = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE
)
subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
sub_path = subscriber.subscription_path(PROJECT_ID, SUB_ID)
data_to_write: list[str] = []
current_listener_records = 0


def write_records_to_file():
    json_data: list[dict] = []

    while len(data_to_write) > 0:
        data_prep = data_to_write.pop()
        json_data.append(json.loads(data_prep))

    if not os.path.exists(SUBSCRIBER_FOLDER):
        os.makedirs(SUBSCRIBER_FOLDER)
    if not os.path.exists(SUBSCRIBER_DATA_PATH_JSON):
        with open(SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
            json.dump(json_data, outfile, indent=4)
    else:
        with open(SUBSCRIBER_DATA_PATH_JSON, "r") as outfile:
            existing_data = json.load(outfile)

        existing_data.extend(json_data)

        with open(SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
            json.dump(existing_data, outfile, indent=4)

    return


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    rcvd_data: bytes = message.data
    decoded_data = rcvd_data.decode()
    data_to_write.append(decoded_data)
    current_listener_records = len(data_to_write)
    if current_listener_records % 1000 == 0:
        print(
            f"Approximate records received so far: {current_listener_records}", end="\r"
        )
    message.ack()
    return


def archive_go():
    streaming_future = subscriber.subscribe(sub_path, callback=callback)
    with subscriber:
        try:
            streaming_future.result(timeout=TIMEOUT)
        except TimeoutError:
            streaming_future.cancel()
            streaming_future.result()
            return


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    pass


if __name__ == "__main__":
    archive_go()
    write_records_to_file()
    upload_blob("a", "a", "a")
    print()
