from pathlib import Path
import logging, os, sys
from time import sleep
from google.oauth2 import service_account
from googleapiclient.discovery import build, MediaFileUpload

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    DATA_MONTH_DAY,
    RAW_DATA_PATH,
    log_and_print,
)

SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
GDRIVE_RAW_DATA_FILES = "1ew5-8Iyrg4gdRdleqRULt5WxMMAiwT8B"

# This script will upload all breadcrumb json responses received from main.py
# to a google drive folder given shared permissions to the service account
# attached to the service account key file located in data_eng_key


def folder_exists(service) -> str | None:
    """Gets the folder ID for the current day being accessed if it exists. Used
    to act as a pseudo boolean in variable verification elsewhere.

    Arguments:
        service -- GDrive service created passed in to be used to access data

    Returns:
        A folder ID if the folder exists, returns None otherwise
    """

    # Create a query to shorten the following lines that includes a mimeType
    # to look for a file of a specific type in the google drive, in this case
    # it's looking for folders.
    query = f"name='{DATA_MONTH_DAY}' and mimeType='application/vnd.google-apps.folder'"

    # If it does it will return the days folder id to be used later
    # otherwise it will return None.
    response = service.files().list(q=query).execute()
    if len(response["files"]) > 0 and bool(response["files"][0]):
        return response["files"][0]["id"]
    return None


def get_folder_files_list(service, gdrive_cur_file_id):
    """Will return all files that exist in the days folder being worked on.

    Arguments:
        service -- Gdrive service passed in provided elsewhere
        gdrive_cur_file_id -- The folder ID retrieved in folder_exists

    Returns:
        Either a constructed set of unique file names, or an empty set.
    """
    gdrive_files_list = set()  # Construct an empty set to use

    # Build the query for the gdrive api
    query = f"'{gdrive_cur_file_id}' in parents"

    # use and execute the query
    response = service.files().list(q=query).execute()

    # From the response, get the key/value pair of "files"
    response = response.get("files")

    # If the response has data, add every file that currently exists in the
    # folder to the set.
    if len(response) > 0:
        for i in range(len(response)):
            gdrive_files_list.add(response[i]["name"])
        return gdrive_files_list

    # Otherwise, return an empty set.
    return set()


def create_gdrive_folder(service, gdrive_folder_to_make):
    """Creates the days gdrive folder if it doesn't exist

    Arguments:
        service -- Gdrive service created elsewhere, passed in here to do work
        gdrive_folder_to_make -- Name of the folder to make

    Returns:
        Will either return the new folders ID, or the folders ID that
        already exists
    """

    # If the folder to make (as verified with folder_exists), is None, make it!
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

        # Return only the id of the folder being accessed after its creation
        return gdrive_folder_to_make

    else:
        log_and_print(
            message=f"Folder already exists on the drive... skipping creation in google drive."
        )
        sleep(0.3)
        # If it does exist, return the folder id retrieved from folder_exists
        return gdrive_folder_to_make


def upload_to_gdrive() -> None:
    """Uploads all files that were saved locally to google drive. This can
    take place independently of main.py, but works best when ran with main.py
    and using -G -P -T -U as the args (-U is the upload)
    """
    logging.info("\n")
    log_and_print(message=f"Starting Google Drive upload.")

    # Create the authorization token
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )

    # Use the authorization token to build the gdrive service connection
    service = build("drive", "v3", credentials=creds)

    # Does the folder to write to exist in the google drive?
    GDRIVE_DATA_MONTH_DAY = folder_exists(service)

    # Even if it does, lets go run this function to make it, or simply return
    # the folder ID we already have.
    GDRIVE_DATA_MONTH_DAY = create_gdrive_folder(service, GDRIVE_DATA_MONTH_DAY)

    # Build a set of files that we know actually exists. This supports O(1)
    # lookup speed when comparing against the files that have to be uploaded
    gdrive_files_list: set = get_folder_files_list(
        service, GDRIVE_DATA_MONTH_DAY
    )

    # Get the local list of files to upload and sort them
    files = os.listdir(RAW_DATA_PATH)
    files.sort()

    # for loop it through all files created in raw_data_files
    num_files = len(files)
    for i in range(num_files):

        # If the file is in the set we created earlier, just log the fact it
        # exists with a short sleep
        if files[i] in gdrive_files_list:
            log_and_print(
                message=f"{files[i]}: already uploaded... skipping.", prend="\r"
            )
            sleep(0.3)

        # Otherwise, upload it!
        else:
            log_and_print(
                message=f"Uploading file# {i+1} out of {num_files}", prend="\r"
            )

            # Build the metadata that needs to be passed along to the creation
            # function. This requires the name of the file, and the folder/path
            # on the gdrive (the gdrive folder ID) to store it to.
            file_metadata = {
                "name": files[i],
                "parents": [GDRIVE_DATA_MONTH_DAY],
            }

            # Create the endpoint to be stored to
            media = MediaFileUpload(RAW_DATA_PATH + "/" + files[i])

            # Upload the current file to the gdrive shared with the service
            # account key

            # body is the file, media_body is the path, and the fields is the
            # data we want to have returned back to the response variable.
            # This is essentially a POST request, but not quite.
            response = (
                service.files()
                .create(body=file_metadata, media_body=media, fields="id")
                .execute()
            )

            # Add the file we just uploaded to the gdrive set so that if there's
            # a chance of it somehow being uploaded again this cycle, it isn't.
            gdrive_files_list.add(files[i])
    logging.info("\n")
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
