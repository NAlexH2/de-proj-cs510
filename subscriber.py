import logging
import json, os
import sys
import time
import traceback
from src.subpipe.store import DataToSQLDB

from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from src.utils.utils import (
    DATA_MONTH_DAY,
    SUBSCRIBER_DATA_PATH_JSON,
    SUBSCRIBER_FOLDER,
    curr_time_micro,
    sub_logger,
)

SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
SUB_ID = "BreadCrumbsRcvr"
TIMEOUT = 1800


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
        self.current_listener_records = 0

    def store_to_sql(self, jData: list[dict]) -> None:
        sub_logger(f"{curr_time_micro()} Sending data to SQL database.")
        db_worker = DataToSQLDB(jData)
        db_worker.to_db_start()
        sub_logger(
            f"{curr_time_micro()} Data transfer to SQL database complete!"
        )

    def write_records_to_file(self):
        json_data: list[dict] = []
        sub_logger(message="")
        sub_logger(
            message=f"{curr_time_micro()} Total records: {self.current_listener_records}"
        )

        while len(self.data_to_write) > 0:
            data_prep = self.data_to_write.pop()
            json_data.append(json.loads(data_prep))

        sub_logger(
            message=f"{curr_time_micro()} Writing all records to a single file."
        )

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

        # Where the db storage magic happens!
        self.store_to_sql(json_data)

        return

    def callback(self, message: pubsub_v1.subscriber.message.Message) -> None:
        rcvd_data: bytes = message.data
        decoded_data = rcvd_data.decode()
        self.data_to_write.append(decoded_data)
        self.current_listener_records = len(self.data_to_write)
        if self.current_listener_records % 1000 == 0:
            sub_logger(
                message=f"{curr_time_micro()} Approximate records received so "
                + f"far: {self.current_listener_records}",
                prend="\r",
            )
        message.ack()
        return

    def subscriber_listener(self):
        sub_logger(
            message=f"\n{curr_time_micro()} Subscriber actively listening..."
        )
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
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/SUBLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    sub_logger(
        message=f"\n{curr_time_micro()} Subscriber sleeping for 20 minutes to allow publisher to publish first"
    )
    time.sleep(1200)

    try:
        sub_worker = PipelineSubscriber()
        start_time = curr_time_micro()
        total_records_overall = 0

        sub_logger(message=f"\n{start_time} Subscriber starting.")

        while True:
            sub_worker.subscriber_listener()

            if sub_worker.current_listener_records > 0:
                sub_worker.write_records_to_file()
                sub_logger(
                    message=f"{curr_time_micro()} Subscriber started at {start_time}. "
                    + f" Subscriber complete."
                )
                total_records_overall += sub_worker.current_listener_records
                sub_worker.current_listener_records = 0

            else:
                sub_logger(
                    message=f"\n{curr_time_micro()} No data received in the past {TIMEOUT//60} minutes."
                )
                total_records_overall += sub_worker.current_listener_records
                sub_worker.current_listener_records = 0

            sub_logger(
                message=f"{curr_time_micro()} Have received and saved "
                + f"{total_records_overall} records up to this point."
            )
            sub_logger(
                message=f"{curr_time_micro()} Subscriber re-starting to continue "
                + f"listening for messages."
            )
            sub_worker.subscriber = pubsub_v1.SubscriberClient(
                credentials=sub_worker.pubsub_creds
            )
            start_time = curr_time_micro()
    except Exception as e:
        logging.basicConfig(
            format="",
            filename=f"logs/SUBLOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.FATAL,
        )
        sub_logger(f"{curr_time_micro()} EXCEPTION THROWN!")
        sub_logger(f"{curr_time_micro()} Traceback:\n{traceback.format_exc()}")
        sub_logger(f"{curr_time_micro()} Exception as e:\n{e}")
