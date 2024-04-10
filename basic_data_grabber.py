import requests
import pandas as pd
import os

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_FILE = "./data_file"

def web_api_response(cf: pd.DataFrame) -> pd.DataFrame:
    all_responses = pd.DataFrame()
    all_responses["ID"] = pd.Series(dtype=str)
    all_responses["Response"] = pd.Series(dtype=str)
    for i in range(cf.size):
        vehicleID = str(cf['Snickers'].at[i])
        resp = requests.request("GET", API_URL+vehicleID)
        if resp.status_code == 404:
            all_responses.loc[all_responses.size] = [str(vehicleID), str(resp.status_code)]
        elif resp.status_code == 202:
            all_responses.loc[all_responses.size] = [str(vehicleID), str(resp.status_code)]
    
    print(all_responses.head(all_responses.size))


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