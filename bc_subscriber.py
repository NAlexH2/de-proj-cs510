import logging
import json, os
import time
import traceback
from src.subpipe.store import BCDataToSQLDB

from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from src.utils.utils import (
    DATA_MONTH_DAY,
    BC_SUBSCRIBER_DATA_PATH_JSON,
    BC_SUBSCRIBER_FOLDER,
    curr_time_micro,
    log_and_print,
)

SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
BC_SUB_ID = "BreadCrumbsRcvr"
TIMEOUT = 1200


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
        self.sub_path = self.subscriber.subscription_path(PROJECT_ID, BC_SUB_ID)
        self.data_to_write: list[str] = []
        self.current_listener_records = 0

    def store_to_sql(self, jData: list[dict]) -> None:
        log_and_print(f"Sending data to SQL database.")
        db_worker = BCDataToSQLDB(jData)
        db_worker.to_db_start()
        log_and_print(f"Data transfer to SQL database complete!")

    def write_records_to_file(self):
        json_data: list[dict] = []
        log_and_print(message="")
        log_and_print(message=f"Total records: {self.current_listener_records}")

        while len(self.data_to_write) > 0:
            data_prep = self.data_to_write.pop()
            json_data.append(json.loads(data_prep))

        log_and_print(message=f"Writing all records to a single file.")

        if not os.path.exists(BC_SUBSCRIBER_FOLDER):
            os.makedirs(BC_SUBSCRIBER_FOLDER)
        if not os.path.exists(BC_SUBSCRIBER_DATA_PATH_JSON):
            with open(BC_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
                json.dump(json_data, outfile, indent=4)
        else:
            with open(BC_SUBSCRIBER_DATA_PATH_JSON, "r") as outfile:
                existing_data = json.load(outfile)

            existing_data.extend(json_data)

            with open(BC_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
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
            log_and_print(
                message=f"Approximate records received so "
                + f"far: {self.current_listener_records}",
                prend="\r",
            )
        message.ack()
        return

    def subscriber_listener(self):
        log_and_print(message=f"Subscriber actively listening...")
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
    log_and_print(
        message=f"Subscriber sleeping for 20 minutes to allow publisher to publish first\n"
    )
    time.sleep(1200)

    try:
        sub_worker = PipelineSubscriber()
        start_time = curr_time_micro()
        total_records_overall = 0

        log_and_print(
            message=f"{start_time} Subscriber '{BC_SUB_ID}' starting.\n"
        )

        while True:
            sub_worker.subscriber_listener()

            if sub_worker.current_listener_records > 0:
                sub_worker.write_records_to_file()
                log_and_print(
                    message=f"Subscriber started at {start_time}. "
                    + f" Subscriber complete."
                )
                total_records_overall += sub_worker.current_listener_records
                sub_worker.current_listener_records = 0

            else:
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
            sub_worker.subscriber = pubsub_v1.SubscriberClient(
                credentials=sub_worker.pubsub_creds
            )
            start_time = curr_time_micro()
    except Exception as e:
        logging.basicConfig(
            format="",
            filename=f"logs/BC-SUBLOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.FATAL,
        )
        log_and_print(f"EXCEPTION THROWN!")
        log_and_print(f"Traceback:\n{traceback.format_exc()}")
        log_and_print(f"Exception as e:\n{e}")
