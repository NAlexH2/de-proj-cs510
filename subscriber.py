import logging
import json, os
import time
import traceback
from src.subpipe.store import DataToSQLDB

from google.oauth2 import service_account
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
from src.utils.utils import (
    DATA_MONTH_DAY,
    BC_SUBSCRIBER_FOLDER,
    BC_SUBSCRIBER_DATA_PATH_JSON,
    SID_SUBSCRIBER_FOLDER,
    SID_SUBSCRIBER_DATA_PATH_JSON,
    curr_time_micro,
    log_and_print,
)

SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
BC_SUB_ID = "BreadCrumbsRcvr"
SID_SUB_ID = "StopDataRcvr"
TIMEOUT = 180


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
        self.bc_path = self.subscriber.subscription_path(PROJECT_ID, BC_SUB_ID)
        self.sid_path = self.subscriber.subscription_path(
            PROJECT_ID, SID_SUB_ID
        )
        self.bc_data_to_write: list[str] = []
        self.bc_current_listener_records = 0

        self.sid_data_to_write: list[str] = []
        self.sid_current_listener_records = 0

    def store_to_sql(self, bc_data: list[dict], sid_data) -> None:
        log_and_print(f"Sending data to SQL database.")
        db_worker = DataToSQLDB(bc_data, sid_data)
        db_worker.to_db_start()
        log_and_print(f"Data transfer to SQL database complete!")

    def write_records_to_file(self, bcJson: list[dict], sidJson: list[dict]):

        if len(bcJson) > 0:
            if not os.path.exists(BC_SUBSCRIBER_FOLDER):
                os.makedirs(BC_SUBSCRIBER_FOLDER)
            log_and_print(
                message=f"Writing all BreadCrumb records to a single file."
            )
            if not os.path.exists(BC_SUBSCRIBER_DATA_PATH_JSON):
                with open(BC_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
                    json.dump(bcJson, outfile, indent=4)
            else:
                with open(BC_SUBSCRIBER_DATA_PATH_JSON, "r") as outfile:
                    existing_data = json.load(outfile)

                existing_data.extend(bcJson)

                with open(BC_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
                    json.dump(existing_data, outfile, indent=4)

        if len(sidJson) > 0:
            if not os.path.exists(SID_SUBSCRIBER_FOLDER):
                os.makedirs(SID_SUBSCRIBER_FOLDER)

            log_and_print(
                message=f"Writing all StopID records to a single file."
            )

            if not os.path.exists(SID_SUBSCRIBER_DATA_PATH_JSON):
                with open(SID_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
                    json.dump(sidJson, outfile, indent=4)
            else:
                with open(SID_SUBSCRIBER_DATA_PATH_JSON, "r") as outfile:
                    existing_data = json.load(outfile)

                existing_data.extend(sidJson)

                with open(SID_SUBSCRIBER_DATA_PATH_JSON, "w") as outfile:
                    json.dump(existing_data, outfile, indent=4)

        return

    def prep_records_writing(self):
        bc_json_data: list[dict] = []
        sid_json_data: list[dict] = []
        log_and_print(message="")
        log_and_print(
            message=f"BreadCrumbs: total records: "
            + f"{self.bc_current_listener_records}"
        )
        log_and_print(
            message=f"BreadCrumbs: total records: "
            + f"{self.sid_current_listener_records}"
        )

        while len(self.bc_data_to_write) > 0:
            bc_data_prep = self.bc_data_to_write.pop()
            bc_json_data.append(json.loads(bc_data_prep))

        while len(self.sid_data_to_write) > 0:
            sid_data_prep = self.sid_data_to_write.pop()
            sid_json_data.append(json.loads(sid_data_prep))

        log_and_print(message=f"Writing all records to a single file.")

        self.write_records_to_file(bcJson=bc_json_data, sidJson=sid_json_data)

        # Where the db storage magic happens!
        self.store_to_sql(bc_json_data, sid_json_data)

        return

    def bc_callback(
        self, message: pubsub_v1.subscriber.message.Message
    ) -> None:
        rcvd_data: bytes = message.data
        decoded_data = rcvd_data.decode()
        self.bc_data_to_write.append(decoded_data)
        self.bc_current_listener_records = len(self.data_to_write)
        if self.current_listener_records % 1000 == 0:
            log_and_print(
                message=f"BreadCrumb Callback: approximate records received so "
                + f"far: {self.bc_current_listener_records}",
                prend="\r",
            )
        message.ack()
        return

    def sid_callback(
        self, message: pubsub_v1.subscriber.message.Message
    ) -> None:
        rcvd_data: bytes = message.data
        decoded_data = rcvd_data.decode()
        self.sid_data_to_write.append(decoded_data)
        self.sid_current_listener_records = len(self.data_to_write)
        if self.current_listener_records % 1000 == 0:
            log_and_print(
                message=f"StopID Callback: approximate records received so "
                + f"far: {self.sid_current_listener_records}",
                prend="\r",
            )
        message.ack()
        return

    def subscriber_listener(self):
        log_and_print(
            message=f"Subscriber actively listening to {BC_SUB_ID} "
            + f"and {SID_SUB_ID}..."
        )
        bc_streaming_future = self.subscriber.subscribe(
            self.bc_path, callback=self.bc_callback
        )
        sid_streaming_future = self.subscriber.subscribe(
            self.bc_path, callback=self.sid_callback
        )
        with self.subscriber:
            try:
                bc_streaming_future.result(timeout=TIMEOUT)
                sid_streaming_future.result(timeout=TIMEOUT)
            except TimeoutError:
                bc_streaming_future.cancel()
                bc_streaming_future.result()
                sid_streaming_future.cancel()
                sid_streaming_future.result()
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
    # time.sleep(1200)

    try:
        sub_worker = PipelineSubscriber()
        start_time = curr_time_micro()
        bc_total_records_overall = 0
        sid_total_records_overall = 0

        log_and_print(message=f"{start_time} Subscriber starting.\n")

        while True:
            sub_worker.subscriber_listener()

            if sub_worker.current_listener_records > 0:
                sub_worker.prep_records_writing()
                log_and_print(
                    message=f"Subscriber started at {start_time}. "
                    + f" Subscriber complete."
                )
                bc_total_records_overall += (
                    sub_worker.bc_current_listener_records
                )
                sid_total_records_overall += (
                    sub_worker.sid_current_listener_records
                )
                sub_worker.current_listener_records = 0

            else:
                log_and_print(
                    message=f"No data received in the past {TIMEOUT//60} minutes.\n"
                )
                bc_total_records_overall += (
                    sub_worker.bc_current_listener_records
                )
                sid_total_records_overall += (
                    sub_worker.sid_current_listener_records
                )
                sub_worker.bc_current_listener_records = 0
                sub_worker.sid_current_listener_records = 0

            log_and_print(
                message=f"BreadCrumb: have received and saved "
                + f"{bc_total_records_overall} records up to this point."
            )
            log_and_print(
                message=f"BreadCrumb: have received and saved "
                + f"{sid_total_records_overall} records up to this point."
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
            filename=f"logs/SUBLOG-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.FATAL,
        )
        logging.error(f"EXCEPTION THROWN!")
        logging.error(f"Traceback:\n{traceback.format_exc()}")
        logging.error(f"Exception as e:\n{e}")
