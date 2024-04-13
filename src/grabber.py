import json
import shutil
import multiprocessing
import requests
import pandas as pd
import os

from datetime import datetime
from src.vars import API_URL, DATA_FOLDER, DATA_MONTH_DAY, DATA_PATH
from typing import Callable, Iterable


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
                self.bad_response = pd.concat(
                    [self.bad_response, to_concat], ignore_index=True
                )
            else:
                self.OK_response = pd.concat(
                    [self.OK_response, to_concat], ignore_index=True
                )
                self.save_JSON_data(resp.text, vehicleID)

        print("\n")
        return

    def save_JSON_data(self, resp_text: str, vehicleID: str) -> None:
        dt_obj = datetime(1, 1, 1)
        json_got = json.dumps(json.loads(resp_text), indent=4)

        file_str = dt_obj.now().strftime("%m-%d-%Y")
        file_str = vehicleID + "-" + file_str + ".json"
        full_file_path = os.path.join(DATA_PATH, file_str)

        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)

    def data_grabber(self) -> None:
        if os.path.exists(DATA_FOLDER):
            shutil.rmtree(DATA_FOLDER)
        os.makedirs(DATA_PATH)
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")
        self.gather_response_codes(csv_frame)

        return
