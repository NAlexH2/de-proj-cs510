import json, os, sys, zlib
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import pubsub_v1, storage
from concurrent.futures import TimeoutError

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import DATA_MONTH_DAY, SUBSCRIBER_DATA_PATH_JSON, SUBSCRIBER_FOLDER

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
    print("sub starting")
    streaming_future = subscriber.subscribe(sub_path, callback=callback)
    with subscriber:
        try:
            streaming_future.result(timeout=TIMEOUT)
        except TimeoutError:
            streaming_future.cancel()
            streaming_future.result()
            return


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client.from_service_account_json(
        json_credentials_path=SERVICE_ACCOUNT_FILE
    )
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0
    blob.upload_from_filename(
        source_file_name, if_generation_match=generation_match_precondition
    )
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")


if __name__ == "__main__":
    # archive_go()
    # write_records_to_file()
    original_dat = open(SUBSCRIBER_DATA_PATH_JSON, "rb").read()
    compressed_data = zlib.compress(original_dat, zlib.Z_BEST_COMPRESSION)
    f = open(
        os.path.join(SUBSCRIBER_FOLDER, DATA_MONTH_DAY + "-compressed.zlib"), "a+b"
    )
    f.write(compressed_data)
    f.close()
    upload_blob(
        "archivetest-bucket",
        os.path.join(SUBSCRIBER_FOLDER, DATA_MONTH_DAY + "-compressed.zlib"),
        f"{DATA_MONTH_DAY}-compressed.zlib",
    )
    print()
