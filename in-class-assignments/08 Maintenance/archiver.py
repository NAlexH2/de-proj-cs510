import pandas as pd
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError

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


if __name__ == "__main__":
    archive_go()
    print()
