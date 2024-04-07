import requests
import pandas as pd
import os

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
BUS_FAMILY = "Whisker"
DATA_FILE = "./data_file"

def web_api_response():
    resp = requests.request("GET", API_URL+BUS_FAMILY)
    if resp.status_code != "200":
        return None
    return resp


def data_grabber():
    file = None
    df = web_api_response()
    if df is not None:
        data_frame = pd.read_csv("./vehicle_ids.csv")
        if os.path.exists(DATA_FILE):
            file = open(DATA_FILE, "x")
    else:
        print("No response data received. Maybe the vehicle group didn't provide service today.")
        return
    
    

if __name__ == "__main__":
    data_grabber()