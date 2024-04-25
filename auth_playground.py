from google.oauth2 import service_account
from googleapiclient.discovery import build
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
GDRIVE_RAW_DATA_FILES = "1ew5-8Iyrg4gdRdleqRULt5WxMMAiwT8B"
PROJECT_ID = "data-eng-419218"
TOPIC_ID = "VehicleData"
SUB_ID = "BreadCrumbsRcvr"


def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    res = message.data.decode("utf-8")
    message.ack()
    return


def time_to_play():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    pubsub_creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )
    publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    # subscriber = pubsub_v1.SubscriberClient(credentials=pubsub_creds)
    # sub_path = subscriber.subscription_path(PROJECT_ID, SUB_ID)
    drive_service = build("drive", "v3", credentials=creds)

    print("\n\nGO PLAY IN THE DEBUGGER!\nTHE WORLD IS YOUR KERNEL!")
    return


if __name__ == "__main__":
    time_to_play()
