import os, sys, json
from google.oauth2 import service_account
from concurrent import futures
from google.cloud import pubsub_v1
from typing import Callable


SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "data-eng-419218"
TOPIC_ID = "VehicleData"


def publish_data(data):
    pubsub_creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )
    publisher = pubsub_v1.PublisherClient(credentials=pubsub_creds)
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    for i in range(working_file_length):
        encoded_message_content = base64.b64encode(
            str(working_file_json[i]).encode()
        ).decode()
        message = {"messages": [{"data": encoded_message_content}]}
        response = (
            service.projects()
            .topics()
            .publish(topic=FULL_TOPIC_ID, body=message)
            .execute()
        )
        records_count += 1
    working_file.close()
    return


if __name__ == "__main__":
    publish_data()
