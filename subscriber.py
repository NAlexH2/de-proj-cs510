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
MAX_MESSAGES = 1000


def pull_subscription_messages(service):
    response = (
        service.projects()
        .subscriptions()
        .pull(subscription=FULL_SUB_ID, body={"maxMessages": MAX_MESSAGES})
        .execute()
    )
    if len(response) > 0:
        return response
    else:
        return None


def save_json_data(received_organized: list[tuple], records_wrote: int):
    # last_epoch = time.time()
    # minute_passed = False
    rcv_len = len(received_organized)
    for i in range(rcv_len):
        file_str = received_organized[i][2] + "-" + mdy_time() + ".json"

        full_file_path = os.path.join(SUBSCRIBER_DATA_PATH, file_str)
        if not os.path.exists(SUBSCRIBER_DATA_PATH):
            os.makedirs(SUBSCRIBER_DATA_PATH)

        if not os.path.exists(full_file_path):
            with open(full_file_path, "w") as outfile:
                json.dump([received_organized[i][1]], outfile, indent=4)
            records_wrote += 1

        else:
            with open(full_file_path, "r") as outfile:
                existing_data = json.load(outfile)

            existing_data.append(received_organized[i][1])

            with open(full_file_path, "w") as outfile:
                json.dump(existing_data, outfile, indent=4)
            records_wrote += 1

        # last_epoch, minute_passed = minute_check(last_epoch)
        # if minute_passed == True:
        #     print(f"Current messages per minute is: {records_wrote//60}.")
        #     minute_passed = False

        return file_str, records_wrote


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


def ack_messages(service, acking_ids: set, response_organized) -> None:
    ack_data = {"ackIds": []}
    for i in range(len(response_organized)):
        ack_data["ackIds"].append(response_organized[i][0])
        acking_ids.add(response_organized[i][0])

    service.projects().subscriptions().acknowledge(
        subscription=FULL_SUB_ID, body=ack_data
    ).execute()
    return acking_ids


def print_random_notice(
    last_file: str,
    curr_file: str,
    resp_total: int,
    records_wrote: int,
) -> None:
    print(
        f"{curr_time_micro()} Random notice: last file was "
        + f"{last_file} and the current file is {curr_file}. "
        + f"Total responses handled: {resp_total}. "
        + f"Total records wrote: {records_wrote} -- pid: {os.getpid()}"
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


# TODO to properly fix the read/write process you will need to not only organize
# all the responses, but create a data structure that can write every single
# response into the file all at once for that vehicle id. Funny enough, the
# best approach might be a dict or json you pass around. Writing that whole
# thing to a file is faster than opening and reading/writing to memory.
def packaged_response(response, ackd_ids):
    organized_response = list(tuple())
    already_grabbed = set()
    received = response["receivedMessages"]
    for i in range(len(received)):
        if received[i]["ackId"] not in ackd_ids:
            decoded_data = decode_message_data(received[i]["message"]["data"])
            vehicle_id = get_vehicle_id(decoded_data)
            organized_response.append(
                (received[i]["ackId"], decoded_data, vehicle_id)
            )
            already_grabbed.add(received[i]["ackId"])

    return organized_response


def minute_check(last_epoch):
    if time.time() - last_epoch >= 60:
        return 0, True
    else:
        return last_epoch, False


def subscriber(service) -> None:
    ackd_ids = set()

    # Number of times a call to time.sleep(60) has happened
    sixty_sleep_count = 0
    response_count = 0
    last_file = None
    curr_file = None
    records_wrote = 0

    print(
        f"{curr_time_micro()} Subscriber w/ pid {os.getpid()} has started."
        + f"Listening for data on GCP Pub/Sub: "
    )
    while True:
        response = pull_subscription_messages(service)
        if response is not None:
            response_count += 1
            sixty_sleep_count = 0
            rand_int = random.randint(1, 500000)
            if response_count % rand_int == 0:
                print_random_notice(
                    last_file, curr_file, response_count, records_wrote
                )
            response_organized = packaged_response(response, ackd_ids)
            curr_file, records_wrote = save_json_data(
                response_organized, records_wrote
            )
            ackd_ids = ack_messages(service, ackd_ids, response_organized)
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
