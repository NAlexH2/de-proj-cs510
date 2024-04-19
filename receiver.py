import base64
from google.oauth2 import service_account
from googleapiclient.discovery import build


SCOPES = ["https://www.googleapis.com/auth/pubsub"]
SERVICE_ACCOUNT_FILE = "./data_eng_key/data-eng-auth-data.json"
PROJECT_ID = "projects/data-eng-419218"
SUB_ID = "projects/data-eng-419218/subscriptions/BreadCrumbsRcvr"


def receiver():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build("pubsub", "v1", credentials=creds)
    # examples
    # service
    # .projects()
    # .subscriptions()
    # .pull(subscription="projects/data-eng-419218/subscriptions/BreadCrumbsRcvr",
    # body={"maxMessages": 10})
    # .execute()


if __name__ == "__main__":
    receiver()
