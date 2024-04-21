import base64, json, os, time, sys, random
from google.oauth2 import service_account
from googleapiclient.discovery import build

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
if "/src" in script_dir:
    from utils import SUBSCRIBER_DATA_PATH, curr_time_micro, mdy_time
else:
    from src.utils import SUBSCRIBER_DATA_PATH, curr_time_micro, mdy_time


SCOPES = ["https://www.googleapis.com/auth/pubsub"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "projects/data-eng-419218"
SUB_ID = "subscriptions/BreadCrumbsRcvr"
FULL_SUB_ID = f"{PROJECT_ID}/{SUB_ID}"


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


def save_json_data(json_got, vehicleID: str):
    file_str = vehicleID + "-" + mdy_time() + ".json"

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


def get_decoded_data(response_data):
    encoded_message_data = response_data["message"]["data"]
    decoded_data = decode_message_data(encoded_message_data)
    return decoded_data


def get_vehicle_id(decoded_data) -> str:
    return str(decoded_data["VEHICLE_ID"])


def ack_message(service, ack_data) -> None:
    service.projects().subscriptions().acknowledge(
        subscription=FULL_SUB_ID, body=ack_data
    ).execute()
    return


def print_random_notice(
    last_file: str, curr_file: str, resp_total: int
) -> None:
    print(
        f"{curr_time_micro()} Random notice: last file was "
        + f"{last_file} and the current file is {curr_file}. "
        + f"Total responses handled: {resp_total} -- {os.getpid()}"
    )
    return


def verbose_append_print(curr_file: str, resp_total: int) -> None:
    print(
        f"{curr_time_micro()} Data appended to {curr_file} and "
        + f"saved - Response#: {resp_total} -- pid: {os.getpid()}"
    )
    return


def print_sixty_sleep(sixty_sleep_count: int, last_file: str) -> None:
    print(
        f"{curr_time_micro()} Sixty sleep counter: {sixty_sleep_count} - "
        + f"No data to receive from topic. Sleeping for 1 "
        + f"minute -- pid: {os.getpid()}"
    )
    if last_file:
        print(
            f"{curr_time_micro()} Last file appended to {last_file} "
            + f"-- pid: {os.getpid()}"
        )
    return


def subscriber(service) -> None:
    ackd_ids = set()

    # Number of times a call to time.sleep(60) has happened
    sixty_sleep_count = 0
    response_count = 1
    last_file = None
    curr_file = None
    print(
        f"{curr_time_micro()} Subscriber w/ pid {os.getpid()} has started."
        + f"Listening for data on GCP Pub/Sub: "
    )
    while True:
        response = pull_subscription_messages(service)
        if response is not None:
            sixty_sleep_count = 0
            rand_int = random.randint(1, 500000)
            if response_count % rand_int == 0:
                print_random_notice(last_file, curr_file, response_count)
            response_data = response.get("receivedMessages")[0]
            to_ack = response_data["ackId"]
            if to_ack not in ackd_ids:
                decoded_data = get_decoded_data(response_data)
                vehicle_id = get_vehicle_id(decoded_data)
                ack_data = {"ackIds": [to_ack]}
                curr_file = save_json_data(decoded_data, vehicle_id)
                ack_message(service, ack_data)
                ackd_ids.add(to_ack)
                if "-V" in sys.argv:
                    verbose_append_print(curr_file, response_count)
            response_count += 1
            last_file = curr_file

        else:
            sixty_sleep_count += 1
            print_sixty_sleep(sixty_sleep_count, last_file)
            time.sleep(60)


def sub_setup():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("pubsub", "v1", credentials=creds)
    subscriber(service)


if __name__ == "__main__":
    sub_setup()
