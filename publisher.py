import os, sys, json, base64
from google.oauth2 import service_account
from googleapiclient.discovery import build
from src.utils import FULL_DATA_PATH, curr_time_micro


SCOPES = ["https://www.googleapis.com/auth/pubsub"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "projects/data-eng-419218"
TOPIC_ID = "topics/VehicleData"
FULL_TOPIC_ID = f"{PROJECT_ID}/{TOPIC_ID}"


def publisher(data_file, total_sent):
    print(
        f"{curr_time_micro()} Publishing file {data_file} -- pid: {os.getpid()}",
        end="\n",
    )
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("pubsub", "v1", credentials=creds)
    full_file_path = os.path.join(FULL_DATA_PATH, data_file)
    working_file = open(full_file_path, "r")
    working_file_json = json.load(working_file)
    working_file_length = len(working_file_json)
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
        total_sent += 1
    working_file.close()

    return


def publish_data():
    total_sent = 0
    files_list = os.listdir(FULL_DATA_PATH)
    files_list.sort()
    file_len = len(files_list)
    for i in range(file_len):
        print(
            f"{curr_time_micro} File {i+1} of {file_len}. Total published: {total_sent} -- pid: {os.getpid()}"
        )
        total_sent += publisher(files_list[i], total_sent)
        print(
            f"{curr_time_micro()} {files_list[i]} published -- pid: {os.getpid()}"
        )

    print(f"All files published")


if __name__ == "__main__":
    publish_data()
