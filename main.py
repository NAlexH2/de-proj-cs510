from typing import Any
import requests
import pandas as pd
import os

from email_notify import emailer

API_URL = "https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id="
DATA_PATH = "./data_files"


class BreadCrumbClass:
    def __init__(self) -> None:
        self.OK_response = self.build_query_df()
        self.bad_response = self.build_query_df()

    def response_vdf(self, vehicleID: str, stat_code: int) -> pd.DataFrame:
        """Build a dataframe just for vehicle responses

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

    def web_api_response(self, cf: pd.DataFrame) -> list[pd.DataFrame]:

        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            resp = requests.request("GET", API_URL + vehicleID)

            # Make a quick dataframe with our info to concat
            to_concat = self.response_vdf(vehicleID, resp.status_code)

            # Collect both responses for notification
            if resp.status_code == 404:
                bad_response = pd.concat([bad_response, to_concat])

            else:
                OK_response = pd.concat([OK_response, to_concat])

        return [OK_response, bad_response]

    def data_grabber(self) -> None:
        file = None
        csv_frame: pd.DataFrame = pd.read_csv("./vehicle_ids.csv")
        df = self.web_api_response(csv_frame)
        oks: pd.DataFrame = df[0]
        bads: pd.DataFrame = df[1]

        # if oks.size is not 0:
        #     os.mkdir(DATA_PATH)
        #     if os.path.exists(DATA_PATH):

        emailer(oks.size, bads.size)


if __name__ == "__main__":
    data_collect = BreadCrumbClass()
    data_collect.data_grabber()
