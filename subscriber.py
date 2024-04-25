import json, os, time, sys, random
from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from src.utils import (
    SUBSCRIBER_DATA_PATH_JSON,
    SUBSCRIBER_FOLDER,
    curr_time_micro,
)

SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
SUB_ID = "BreadCrumbsRcvr"
TIMEOUT = 120


class PipelineSubscriber:
    def __init__(self) -> None:
        self.pubsub_creds = (
            service_account.Credentials.from_service_account_file(
                SERVICE_ACCOUNT_FILE
            )
        )
        self.subscriber = pubsub_v1.SubscriberClient(
            credentials=self.pubsub_creds
        )
        self.sub_path = self.subscriber.subscription_path(PROJECT_ID, SUB_ID)
        self.data_to_write: list[str] = []
        self.total_records = 0

    def write_records_to_file(self):
        json_data = []
        print()
        print(f"{curr_time_micro()} Total records: {self.total_records}")
        print(f"{curr_time_micro()} Writing all records to a single file.")

        while len(self.data_to_write) > 0:
            data_prep = self.data_to_write.pop()
            json_data.append(json.loads(data_prep))

        # json_data = json.dumps(json_data)

        if not os.path.exists(SUBSCRIBER_FOLDER):
            os.makedirs(SUBSCRIBER_FOLDER)
        with open(SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
            json.dump(json_data, outfile, indent=4)

        return

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        rcvd_data: bytes = message.data
        decoded_data = rcvd_data.decode()
        self.data_to_write.append(decoded_data)
        self.total_records += 1
        if self.total_records % 1000 == 0:
            print(
                f"{curr_time_micro()} Approximate records so far: {self.total_records}",
                end="\r",
            )
        message.ack()
        return

    def subscriber_listener(self):
        while True:
            streaming_future = self.subscriber.subscribe(
                self.sub_path, callback=self.callback
            )
            with self.subscriber:
                try:
                    streaming_future.result(timeout=TIMEOUT)
                except TimeoutError:
                    streaming_future.cancel()
                    streaming_future.result()
                    return


if __name__ == "__main__":
    sub_worker = PipelineSubscriber()
    start_time = curr_time_micro()
    print(f"{start_time} Subscriber starting.")
    sub_worker.subscriber_listener()
    sub_worker.write_records_to_file()
    print(f"{curr_time_micro()} Subscriber started at {start_time}.")
    sys.exit(0)
