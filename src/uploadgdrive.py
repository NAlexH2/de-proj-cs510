import os
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ['https://www.googleapis.com/auth/drive']
SERVICE_ACCOUNT_FILE = "../data_eng_key/data-eng-auth-data.json"


def write_to_gdrive() -> None:
    serv_acc = service_account.Credentials()
    creds = serv_acc.from_service_account_file(SERVICE_ACCOUNT_FILE, 
                                           scopes=SCOPES)

    drive_service = build('drive', 'v3', credentials=creds)
    print(type(drive_service))
    
    