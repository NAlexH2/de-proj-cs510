import requests
import pandas as pd
import os

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_FILE = "./data_file"

def web_api_response(cf: pd.DataFrame) -> pd.DataFrame:
    for i in range(cf.size):
        resp = requests.request("GET", API_URL+str(cf.loc[i][0]))
    if resp.status_code != "200":
        return None
    return resp


def data_grabber() -> None:
    file = None
    csv_frame: pd.DataFrame = pd.read_csv("./vehicle_ids.csv")
    df = web_api_response(csv_frame)
    if df is not None:
        if os.path.exists(DATA_FILE):
            file = open(DATA_FILE, "x")
    else:
        print("No response data received. Maybe the vehicle group didn't provide service today.")
        return
    
    

if __name__ == "__main__":
    data_grabber()