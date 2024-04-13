from datetime import datetime
import os

# Utilities used throughout
API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_FOLDER = os.path.join("raw_data_files")
DATA_MONTH_DAY = datetime.now().strftime("%m-%d")
DATA_PATH = os.path.join(DATA_FOLDER, DATA_MONTH_DAY)

# Used in uploadgdrive.py
SCOPES = ["https://www.googleapis.com/auth/drive"]
SERVICE_ACCOUNT_FILE = "../data_eng_key/data-eng-auth-data.json"
