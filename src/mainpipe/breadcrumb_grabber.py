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
from src.mainpipe.breadcrumb_publisher import PipelinePublisherBC


class DataGrabberBC:
    """Acts as the data collector from the vehicle bus data api

    pub_worker - An object from PipeLinePublisherBC already created elsewhere
    to be accessed and manipulated to allow the seamless transfer of data
    from gatherer to publisher.
    """

    def __init__(self, pub_worker: PipelinePublisherBC) -> None:
        self.pub_worker: PipelinePublisherBC = pub_worker

    def gather_data(self, cf: pd.DataFrame):
        """Collect data from API endpoint accessing each vehicle id in the
        dataframe to be used as the final part of the api call

        Arguments:
            cf -- dataframe with vehicle id list.
        """
        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            log_and_print(message=f"VID: {vehicleID}", prend="\r")

            resp = requests.request("GET", BREADCRUMB_API_URL + vehicleID)

            if resp.status_code != 404:  # Only keep the good ones!

                # Write the api content received to a .json file
                self.save_json_data(resp.text, vehicleID)

                # If we plan on publishing, give the same data to our
                # shared publisher object.
                if "-P" in sys.argv:
                    self.pub_worker.add_to_publish_list(resp.text)
        return

    def save_json_data(self, resp_text: str, vehicleID: str) -> None:
        # Take the response, json.loads (because it's bytes) with some
        # indentation to keep it pretty when the dumps receives it and saves it
        # a var
        json_got = json.dumps(resp_text, indent=4)

        # Build the file string name
        file_str = vehicleID + "-" + mdy_string() + ".json"

        # Use system agnostic function call to build the path
        full_file_path = os.path.join(RAW_DATA_PATH, file_str)

        # Write it to disk with the formatted string, the filepath created, and
        # the filename we created too!
        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)

    def data_grabber_main(self) -> None:
        # This function acts as the staging area for independent and combined
        # running with the main.py app.
        if os.path.exists(RAW_DATA_PATH):
            shutil.rmtree(RAW_DATA_PATH)
        os.makedirs(RAW_DATA_PATH)

        # Save the csv of all vehicle data to a pandas dataframe to make it
        # easier to traverse and access each value during the api call
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")

        # Begin the collection passing in our new df
        self.gather_data(csv_frame)
        log_and_print(message="Gathering complete.")

        return


if __name__ == "__main__":
    # This nameguard allows the collector to run entirely independent and not
    # publish the data, just write to disk. This is to assist in the modularity
    # of the pipeline throughout.
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        format="",
        filename=f"logs/GRABBER_LOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    log_and_print(message="Gathering staring.")
    grabber = DataGrabberBC(pub_worker=None)
    grabber.data_grabber_main()
