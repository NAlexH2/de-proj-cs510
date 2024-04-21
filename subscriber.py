import base64, json, os, time, sys, multiprocessing
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


def save_json_data(json_got, vehicleID: str) -> None:
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


def get_vehicle_id(decoded_data):
    return str(decoded_data["VEHICLE_ID"])


def ack_message(service, ack_data):
    service.projects().subscriptions().acknowledge(
        subscription=FULL_SUB_ID, body=ack_data
    ).execute()


def same_files(last_file, curr_file):
    return last_file is not None and last_file != curr_file


def new_file_switch(last_file, curr_file):
    return (
        f"{curr_time_micro()} - Files have switched. Last file appended to "
        + f"{last_file}. New file being worked on is {curr_file}. "
        + "This probably is either because the subscriber finished working on that file "
        + "and there were no more messages associated to that file, or messages aren't "
        + f"in any particular order. -- pid: {os.getpid()}"
    )


def multithreaded_subscriber(service):
    ackd_ids = set()
    sixty_sleep_count = 0
    response_not_none = 1
    last_file = None
    curr_file = None
    print(
        f"{curr_time_micro()} - Subscriber w/ following pid has started listening: {os.getpid()}"
    )
    while True:
        # Number of times a call to time.sleep(60) has happened

        # prime the pump, so to speak, before getting all the data
        response = pull_subscription_messages(service)
        if response is not None:
            print(os.getpid(), curr_file)
            sixty_sleep_count = 0
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
                    print(
                        f"{curr_time_micro()} - Data appended to {curr_file} and "
                        + f"saved - Response#: {response_not_none} -- pid: {os.getpid()}"
                    )
            # Prime the pump for the next loop, cycle to the next possible message
            # that may be waiting for us.
            response_not_none += 1

            if same_files(last_file, curr_file):
                pass
                # print(new_file_switch(last_file, curr_file))
            last_file = curr_file
            time.sleep(0.05)

        else:
            sixty_sleep_count += 1
            print(
                f"{curr_time_micro()} - Sixty sleep counter: {sixty_sleep_count} - No data "
                + f"to receive from topic. Sleeping for 1 minute -- pid: {os.getpid()}"
            )
            if last_file is not None:
                print(
                    f"{curr_time_micro()} - Last file appended to {curr_file} "
                    + f"-- pid: {os.getpid()}"
                )
            time.sleep(60)


def work_multi(subs_list, num_procs):
    p = multiprocessing.Pool(num_procs)

    # maps each file name over one of the processors available. This is not done
    # in order, and the file is chosen by the multiprocessing library in some
    # fashion.
    p.imap(multithreaded_subscriber, subs_list)

    # Just gotta clean up once all files have been worked on.
    p.close()
    p.join()


def multithreaded_subscriber__setup():
    concurrences = 2
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    subscriber_one = build("pubsub", "v1", credentials=creds)
    subscriber_two = build("pubsub", "v1", credentials=creds)
    subs_list = [subscriber_one, subscriber_two]

    work_multi(subs_list, concurrences)
    print(f"Terminating app...")


if __name__ == "__main__":
    multithreaded_subscriber__setup()
