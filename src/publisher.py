import multiprocessing
import os, sys, json, base64
from google.oauth2 import service_account
from googleapiclient.discovery import build


script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

if "/src" in script_dir:
    from utils import FULL_DATA_PATH, curr_time_micro
else:
    from src.utils import FULL_DATA_PATH, curr_time_micro


SCOPES = ["https://www.googleapis.com/auth/pubsub"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "projects/data-eng-419218"
TOPIC_ID = "topics/VehicleData"
FULL_TOPIC_ID = f"{PROJECT_ID}/{TOPIC_ID}"


def multithread_publisher(data_file):
    print(
        f"{curr_time_micro()} - Publishing file {data_file} -- pid: {os.getpid()}"
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
        if len(response["messageIds"]) > 0 and "-V" in sys.argv:
            print(
                f"{data_file} - File record {i+1} of {working_file_length} "
                + f"published w/ id {response['messageIds'][0]} "
                + f"-- pid: {os.getpid()}"
            )
    print(f"{curr_time_micro()} - {data_file} published -- pid: {os.getpid()}")
    working_file.close()

    return


def work_multi(files_list, num_procs):
    p = multiprocessing.Pool(num_procs)

    # maps each file name over one of the processors available. This is not done
    # in order, and the file is chosen by the multiprocessing library in some
    # fashion.
    p.imap_unordered(multithread_publisher, files_list)

    # Just gotta clean up once all files have been worked on.
    p.close()
    p.join()


def publish_data():
    concurrences = 2

    files_list = os.listdir(FULL_DATA_PATH)
    files_list.sort()
    work_multi(files_list, concurrences)

    print(f"All files published")


if __name__ == "__main__":
    publish_data()
