import json
import multiprocessing
from typing import Callable, Iterable
import requests
import pandas as pd
import os
from datetime import datetime

from .email_notify import emailer

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_PATH = "./data_files"


class DataGrabber:
    """
    Collection of data from bus API that the breadcrumbs come from.

        Class Data Members:
            OK_response -- pandas dataframe that stores the 200 status
            codes

            bad_response -- pandas dataframe that stores the 404 status
            codes
    """

    def __init__(self) -> None:
        self.OK_response = self.build_query_df()
        self.bad_response = self.build_query_df()

    def paralleled_ops(func: Callable, data: Iterable):
        # TODO: come back to this once you've figured out the algo for collecting
        # data

        # p = multiprocessing.Pool(multiprocessing.cpu_count() // 2)
        # for result in p.imap(func, data):
        pass

    def response_vdf(self, vehicleID: str, stat_code: int) -> None:
        """Build a dataframe just for vehicle responses to self modify

        Arguments:
            vehicleID -- id of vehicle used in the query operation

            stat_code -- response status code received in query operation

        Returns:
            pd.DataFrame with ID and Response as columns and a single row
        """

        data = {
            "ID": [str(vehicleID)],
            "Response": [str(stat_code)],
        }
        return pd.DataFrame(data)

    def build_query_df(self) -> pd.DataFrame:
        """Generates an empty dataframe with 'ID' and 'Response' columns.

        Returns:
            pd.DataFrame with columns 'ID' and 'Response'.
        """

        new_df = pd.DataFrame()
        new_df["ID"] = pd.Series(dtype=str)
        new_df["Response"] = pd.Series(dtype=str)
        return new_df

    def gather_response_codes(self, cf: pd.DataFrame) -> list[pd.DataFrame]:

        # TODO: parallelize this operation to make it go faster
        # TODO: docstring
        print("\n")
        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            print(f"VID: {vehicleID}", end="\r")

            resp = requests.request("GET", API_URL + vehicleID)

            # Make a quick dataframe with our info to concat
            to_concat = self.response_vdf(vehicleID, resp.status_code)

            # Collect both responses for notification
            if resp.status_code == 404:
                self.bad_response = pd.concat([self.bad_response, to_concat])
            else:
                self.OK_response = pd.concat([self.OK_response, to_concat])

        return

    def get_JSON_data(self, ok_df: pd.DataFrame) -> list[dict]:
        # TODO: parallelize this operation too? I don't see why we can't...
        for i in range(ok_df.size):
            vehicleID = str(ok_df["ID"][i].values[0])
            resp = requests.request("GET", API_URL + vehicleID)
            json_got = json.loads(resp.text)

            dt_obj = datetime(1, 1, 1)
            file_str = dt_obj.now().strftime("%m/%d/%Y-%H:%M:%S:%f")[:-3]
            return

    def save_JSON_data(self) -> None:
        pass

    def data_grabber(self) -> None:
        file = None
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")
        self.gather_response_codes(csv_frame)

        if self.OK_response.size != 0:
            if os.path.exists(DATA_PATH):
                os.rmdir(DATA_PATH)
            os.mkdir(DATA_PATH)
            self.get_JSON_data(self.OK_response)

        return
