import os
from google.oauth2 import service_account
from googleapiclient.discovery import build, MediaFileUpload

from src.vars import DATA_MONTH_DAY

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
GDRIVE_RAW_DATA_FILES = "1ew5-8Iyrg4gdRdleqRULt5WxMMAiwT8B"


def folder_exists(service):
    query = f"name='{DATA_MONTH_DAY}' and mimeType='application/vnd.google-apps.folder'"
    response = service.files().list(q=query).execute()
    if len(response["files"]) > 0 and bool(response["files"][0]):
        return response["files"][0]["id"]
    return None


def file_exists(service, file_name):
    query = f"name='{file_name}'"
    response = service.files().list(q=query).execute()
    if len(response["files"]) > 0 and bool(response["files"][0]):
        return response["files"][0]["id"]
    return None


def upload_to_gdrive() -> None:

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("drive", "v3", credentials=creds)
    GDRIVE_DATA_MONTH_DAY = folder_exists(service)
    if GDRIVE_DATA_MONTH_DAY is None:
        folder_metadata = {
            "name": DATA_MONTH_DAY,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [GDRIVE_RAW_DATA_FILES],
        }
        GDRIVE_DATA_MONTH_DAY = (
            service.files().create(body=folder_metadata, fields="id").execute()
        )

    files = os.listdir("./raw_data_files/" + DATA_MONTH_DAY)
    files.sort()

    # for loop it through all files created in raw_data_files
    num_files = len(files)
    for i in range(num_files):
        exists = file_exists(service, files[i])

        if exists is None:
            print(f"Uploading file# {i+1} out of {num_files+1}", end="\r")

            file_metadata = {"name": files[i], "parents": [GDRIVE_DATA_MONTH_DAY]}
            media = MediaFileUpload(
                "./raw_data_files/" + DATA_MONTH_DAY + "/" + files[i]
            )

            # Upload the current file to my pdx gdrive
            response = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )

    print("\n")

    return


if __name__ == "__main__":
    upload_to_gdrive()
