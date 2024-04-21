import base64, json, os, time
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

from src.vars import SUBSCRIBER_DATA_PATH


SCOPES = ["https://www.googleapis.com/auth/pubsub"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "projects/data-eng-419218"
SUB_ID = "subscriptions/BreadCrumbsRcvr"
FULL_SUB_ID = f"{PROJECT_ID}/{SUB_ID}"
DT_OBJ = datetime(1, 1, 1)


def pull_subscription_messages(service):
    response = (
        service.projects()
        .subscriptions()
        .pull(subscription=FULL_SUB_ID, body={"maxMessages": 1})
        .execute()
    )
    if len(response) > 0:
        return response
    else:
        return None


def save_json_data(json_got, vehicleID: str) -> None:
    file_str = DT_OBJ.now().strftime("%m-%d-%Y")
    file_str = vehicleID + "-" + file_str + ".json"

    full_file_path = os.path.join(SUBSCRIBER_DATA_PATH, file_str)
    if not os.path.exists(SUBSCRIBER_DATA_PATH):
        os.makedirs(SUBSCRIBER_DATA_PATH)
    if not os.path.exists(full_file_path):
        with open(full_file_path, "w") as outfile:
            json.dump([json_got], outfile, indent=4)
    else:
        with open(full_file_path, "r") as outfile:
            existing_data = json.load(outfile)

        existing_data.append(json_got)

        with open(full_file_path, "w") as outfile:
            json.dump(existing_data, outfile, indent=4)
    return file_str


def decode_message_data(encoded_message_data):
    decoded_message = base64.b64decode(encoded_message_data).decode()
    decoded_message = decoded_message.replace("'", '"')
    decoded_message = decoded_message.replace("None", '"None"')
    decoded_data = json.loads(decoded_message)
    return decoded_data


def receiver():
    ackd_ids = set()
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("pubsub", "v1", credentials=creds)
    sixty_sleep_count = 0
    response_not_none = 1
    while True:
        # Number of times a call to time.sleep(60) has happened

        # prime the pump, so to speak, before getting all the data
        response = pull_subscription_messages(service)
        while response is not None:
            sixty_sleep_count = 0
            response_data = response.get("receivedMessages")[0]
            to_ack = response_data["ackId"]
            if to_ack not in ackd_ids:
                encoded_message_data = response_data["message"]["data"]
                ack_data = {"ackIds": [to_ack]}
                decoded_data = decode_message_data(encoded_message_data)
                vehicle_id = str(decoded_data["VEHICLE_ID"])
                file_worked_on = save_json_data(decoded_data, vehicle_id)
                service.projects().subscriptions().acknowledge(
                    subscription=FULL_SUB_ID, body=ack_data
                ).execute()
                ackd_ids.add(to_ack)
                dt_output = DT_OBJ.now().strftime("%m-%d-%Y-%H:%M:%S.%f")[:-3]
                print(
                    f"{dt_output} - Data appended to {file_worked_on} and "
                    + f"saved - Response#: {response_not_none}"
                )

            # Prime the pump for the next loop, cycle to the next possible message
            # that may be waiting for us.
            response_not_none += 1
            response = pull_subscription_messages(service)
        sixty_sleep_count += 1
        dt_output = DT_OBJ.now().strftime("%m-%d-%Y-%H:%M:%S.%f")[:-3]
        print(
            f"{dt_output} - Sixty sleep counter: {sixty_sleep_count} - No data "
            + "to receive from topic. Sleeping for 1 minute."
        )
        time.sleep(60)


if __name__ == "__main__":
    receiver()
