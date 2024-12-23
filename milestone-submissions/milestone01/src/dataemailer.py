import base64
import os, sys, tarfile
from email.message import EmailMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

if "/src" in script_dir:
    from utils import DATA_MONTH_DAY, FULL_DATA_PATH, curr_time_micro
else:
    from src.utils import DATA_MONTH_DAY, FULL_DATA_PATH, curr_time_micro


def tar_data():
    file_exists = os.path.exists(f"{FULL_DATA_PATH}.tar")
    if file_exists:
        os.remove(f"{FULL_DATA_PATH}.tar")
    tar = tarfile.open(f"{FULL_DATA_PATH}.tar", "w")
    files = os.listdir(f"{FULL_DATA_PATH}")
    files.sort()
    for file in files:
        file_path = os.path.join(FULL_DATA_PATH, file)
        tar.add(file_path, arcname=file_path)
        print(f"{curr_time_micro()} {file} added to tar.")
    tar.close()
    print(f"{curr_time_micro()} tar for {DATA_MONTH_DAY} complete.")


def data_emailer(ok_size: int, bad_size: int) -> None:
    SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
    SCOPES = [
        "https://www.googleapis.com/auth/gmail.send",
        "https://www.googleapis.com/auth/gmail.compose",
    ]
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    creds = creds.with_subject("nharris@pdx.edu")
    service = build("gmail", "v1", credentials=creds)
    message = EmailMessage()
    message.set_content(f"{DATA_MONTH_DAY}---Data Retrieval")
    message["To"] = "nharris@pdx.edu"
    message["From"] = creds.service_account_email
    message["Subject"] = f"{DATA_MONTH_DAY} Data Retrieval2"

    encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    create_message = {"raw": encoded_message}
    send_message = (
        service.users()
        .messages()
        .send(userId="me", body=create_message)
        .execute()
    )
    return


if __name__ == "__main__":
    tar_data()
    # data_emailer(0, 0)
