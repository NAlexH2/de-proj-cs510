import json, shutil, requests, pandas as pd, os, sys, logging
from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    BREADCRUMB_API_URL,
    RAW_DATA_PATH,
    DATA_MONTH_DAY,
    mdy_string,
    log_and_print,
)
from src.mainpipe.breadcrumb_publisher import PipelinePublisher


class DataGrabber:
    """
    Collection of data from bus API that the breadcrumbs come from.

        Class Data Members:
            OK_response -- pandas dataframe that stores the 200 status
            codes

            bad_response -- pandas dataframe that stores the 404 status
            codes
    """

    def __init__(self, pub_worker: PipelinePublisher) -> None:
        self.OK_response = self.build_query_df()
        self.bad_response = self.build_query_df()
        self.pub_worker: PipelinePublisher = pub_worker

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

    def gather_data(self, cf: pd.DataFrame):
        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            log_and_print(message=f"VID: {vehicleID}", prend="\r")

            resp = requests.request("GET", BREADCRUMB_API_URL + vehicleID)

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
                self.save_json_data(resp.text, vehicleID)
                if "-P" in sys.argv:
                    self.pub_worker.add_to_publish_list(resp.text)

        return

    def save_json_data(self, resp_text: str, vehicleID: str) -> None:
        json_got = json.dumps(json.loads(resp_text), indent=4)

        file_str = vehicleID + "-" + mdy_string() + ".json"
        full_file_path = os.path.join(RAW_DATA_PATH, file_str)

        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)

    def data_grabber_main(self) -> None:
        if os.path.exists(RAW_DATA_PATH):
            shutil.rmtree(RAW_DATA_PATH)
        os.makedirs(RAW_DATA_PATH)
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")
        self.gather_data(csv_frame)
        log_and_print(message="Gathering complete.")

        return


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/GRABBER_LOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    log_and_print(message="Gathering staring.")
    grabber = DataGrabber(pub_worker=None)
    grabber.data_grabber_main()
