import requests
import pandas as pd

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="

def data_graber():
    data_frame = pd.read_csv("./vehicle_ids.csv")
    

if __name__ == "__main__":
    data_graber()