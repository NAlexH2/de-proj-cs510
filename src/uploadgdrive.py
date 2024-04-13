import os
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from vars import SCOPES, SERVICE_ACCOUNT_FILE

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
FOLDER_ID = "1ew5-8Iyrg4gdRdleqRULt5WxMMAiwT8B"


def upload_to_gdrive() -> None:
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    d_service: Resource = build("drive", "v3", credentials=creds)


if __name__ == "__main__":
    upload_to_gdrive()
