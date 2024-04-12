import os

from datetime import datetime
import shutil
from src.grabber import DataGrabber
from src.uploadgdrive import write_to_gdrive

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_FOLDER = os.path.join("raw_data_files")
DATA_MONTH_DAY = datetime.now().strftime("%m-%d")
DATA_PATH = os.path.join(DATA_FOLDER, DATA_MONTH_DAY)

if __name__ == "__main__":
    data_collect = DataGrabber()
    data_collect.data_grabber()
    # write_to_gdrive()

    # shutil.rmtree(DATA_FOLDER)
    
