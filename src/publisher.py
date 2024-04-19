import os
import sys
import json
import base64
from time import sleep
from google.oauth2 import service_account
from googleapiclient.discovery import build
import sys

SCOPES = ["https://www.googleapis.com/auth/pubsub"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "projects/data-eng-419218"
TOPIC_ID = "projects/data-eng-419218/topics/VehicleData"


def publisher():

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("pubsub", "v1", credentials=creds)
    message_content = "hello world"
    encoded_message_content = base64.b64encode(
        message_content.encode()
    ).decode()
    message_data = {"messages": [{"data": encoded_message_content}]}

    # Convert the message data to a JSON string
    message = message_data
    return


if __name__ == "__main__":
    publisher()
