import os, sys, json, base64
from google.oauth2 import service_account
from googleapiclient.discovery import build

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

if "/src" in script_dir:
    from vars import DATA_MONTH_DAY, FULL_DATA_PATH
else:
    from src.vars import DATA_MONTH_DAY, FULL_DATA_PATH

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

    # TODO: before you continue make sure you encode each files json data in base64
    # like this:
    # base64.b64encode(str(message_data).encode()).decode()
    # then attach that to the body of the json object being passed in to the
    # example below.
    message_data = {"messages": [{"data": encoded_message_content}]}

    # Convert the message data to a JSON string
    message = message_data
    # publish example
    # service
    # .projects()
    # .topics()
    # .publish(topic=TOPIC_ID, body=message)

    files = os.listdir(FULL_DATA_PATH)
    files.sort()

    # for loop it through all files created in raw_data_files
    num_files = len(files)
    for i in range(num_files):
        pass
    return


if __name__ == "__main__":
    publisher()
