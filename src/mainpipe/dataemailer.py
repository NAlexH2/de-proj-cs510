import base64
import os, sys, tarfile
from email.message import EmailMessage
from google.oauth2 import service_account
from googleapiclient.discovery import build

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))

if "/src" in script_dir:
    from utils.utils import DATA_MONTH_DAY, curr_time_micro
else:
    from src.utils.utils import DATA_MONTH_DAY, curr_time_micro


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
    data_emailer(0, 0)
