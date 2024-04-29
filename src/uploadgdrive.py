import logging, os, sys
from time import sleep
from google.oauth2 import service_account
from googleapiclient.discovery import build, MediaFileUpload

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
if "/src" in script_dir:
    from utils import (
        DATA_MONTH_DAY,
        FULL_DATA_PATH,
        log_or_print,
        curr_time_micro,
    )
else:
    from src.utils import (
        DATA_MONTH_DAY,
        FULL_DATA_PATH,
        log_or_print,
        curr_time_micro,
    )

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
GDRIVE_RAW_DATA_FILES = "1ew5-8Iyrg4gdRdleqRULt5WxMMAiwT8B"


def folder_exists(service):
    query = f"name='{DATA_MONTH_DAY}' and mimeType='application/vnd.google-apps.folder'"
    response = service.files().list(q=query).execute()
    if len(response["files"]) > 0 and bool(response["files"][0]):
        return response["files"][0]["id"]
    return None


def get_folder_files_list(service, gdrive_cur_file_id):
    gdrive_files_list = set()
    query = f"'{gdrive_cur_file_id}' in parents"
    response = service.files().list(q=query).execute()
    response = response.get("files")
    if len(response) > 0:
        for i in range(len(response)):
            gdrive_files_list.add(response[i]["name"])
        return gdrive_files_list
    return set()


def create_gdrive_folder(service, gdrive_folder_to_make):
    if gdrive_folder_to_make is None:
        folder_metadata = {
            "name": DATA_MONTH_DAY,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [GDRIVE_RAW_DATA_FILES],
        }
        gdrive_folder_to_make = (
            service.files().create(body=folder_metadata, fields="id").execute()
        )
        gdrive_folder_to_make = gdrive_folder_to_make["id"]
        return gdrive_folder_to_make
    else:
        log_or_print(
            message=f"{curr_time_micro()} Folder already exists on the drive... skipping creation in google drive."
        )
        sleep(0.3)
        return gdrive_folder_to_make


def upload_to_gdrive() -> None:
    log_or_print(message="", use_print=True, prend="\n")
    log_or_print(
        message=f"{curr_time_micro()} Starting Google Drive upload.",
        use_print=True,
        prend="\n",
    )
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    service = build("drive", "v3", credentials=creds)
    GDRIVE_DATA_MONTH_DAY = folder_exists(service)
    GDRIVE_DATA_MONTH_DAY = create_gdrive_folder(service, GDRIVE_DATA_MONTH_DAY)

    gdrive_files_list: set = get_folder_files_list(
        service, GDRIVE_DATA_MONTH_DAY
    )
    files = os.listdir(FULL_DATA_PATH)
    files.sort()

    # for loop it through all files created in raw_data_files
    num_files = len(files)
    for i in range(num_files):
        if files[i] in gdrive_files_list:
            log_or_print(
                message=f"{curr_time_micro()} {files[i]}: already uploaded... skipping.",
                use_print=True,
                prend="\r",
            )
            sleep(0.3)
        else:
            log_or_print(
                message=f"{curr_time_micro()} Uploading file# {i+1} out of {num_files}",
                use_print=True,
                prend="\r",
            )

            file_metadata = {
                "name": files[i],
                "parents": [GDRIVE_DATA_MONTH_DAY],
            }
            media = MediaFileUpload(FULL_DATA_PATH + "/" + files[i])

            # Upload the current file to my pdx gdrive
            response = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )
            gdrive_files_list.add(files[i])
    log_or_print(message="", use_print=True, prend="\n")
    return


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/UPLOADLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    upload_to_gdrive()
