import json, shutil, multiprocessing, requests, pandas as pd, os, sys
from typing import Callable, Iterable

script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
if "/src" in script_dir:
    from utils import API_URL, FULL_DATA_PATH, mdy_time, curr_time_micro
    from publisher import PipelinePublisher
else:
    from src.utils import API_URL, FULL_DATA_PATH, mdy_time, curr_time_micro
    from src.publisher import PipelinePublisher


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

    def gather_data(self, cf: pd.DataFrame):
        # TODO: parallelize this operation to make it go faster
        # TODO: docstring
        # TODO: bool to set to False once a 200 response is received
        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            print(f"{curr_time_micro()} VID: {vehicleID}", end="\r")

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
                self.save_json_data(resp.text, vehicleID)
                self.pub_worker.add_to_publish_list(resp.text)

        return

    def save_json_data(self, resp_text: str, vehicleID: str) -> None:
        json_got = json.dumps(json.loads(resp_text), indent=4)

        file_str = vehicleID + "-" + mdy_time() + ".json"
        full_file_path = os.path.join(FULL_DATA_PATH, file_str)

        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)

    def data_grabber_main(self) -> None:
        if os.path.exists(FULL_DATA_PATH):
            shutil.rmtree(FULL_DATA_PATH)
        os.makedirs(FULL_DATA_PATH)
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")
        self.gather_data(csv_frame)

        return


if __name__ == "__main__":
    grabber = DataGrabber()
    grabber.data_grabber_main()
