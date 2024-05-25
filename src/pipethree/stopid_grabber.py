import time
from bs4 import BeautifulSoup, ResultSet, Tag
import json, os, sys, logging, requests, re
import pandas as pd

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    STOPID_DATA_FOLDER,
    STOPID_API_URL,
    STOPID_DATA_PATH,
    DATA_MONTH_DAY,
    mdy_string,
    log_and_print,
)
from src.pipethree.stopid_publisher import PipelinePublisherSID


class DataGrabberSID:
    """Acts as the data collector from the Stop ID data api"""

    def __init__(self, pub_worker: PipelinePublisherSID) -> None:
        # Store the collected data in memory prior to writing to file
        self.html_to_dict_data: list[dict] = None

        # Init the pubworker to share data
        self.pub_worker: PipelinePublisherSID = pub_worker

    def gather_data(self, cf: pd.DataFrame):
        """Collect data from API endpoint accessing each vehicle id in the
        dataframe to be used as the final part of the api call

        Arguments:
            cf -- dataframe with vehicle id list.
        """
        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            log_and_print(message=f"VID: {vehicleID}", prend="\r")
            try:  # Try this to prevent any hard crashes on failed requests
                resp: requests.Response = requests.request(
                    "GET", STOPID_API_URL + vehicleID, timeout=15
                )
            except requests.ConnectTimeout:
                log_and_print(
                    message=f"{vehicleID} request failed\n",
                )
                continue

            # Using bs4, convert the response into something that can be
            # easily parsed with a BeautifulSoup object
            soup: BeautifulSoup = BeautifulSoup(resp.content, "html.parser")

            if resp.status_code != 404:  # Only keep the good ones!
                # First, convert the html response into a json like object
                # to easily access in the future post publish
                self.html_to_json_like(soup)

                # Save to disk
                self.save_to_json(vehicleID)

                # If we plan on publishing, give the same data to our
                # shared publisher object.
                if "-P" in sys.argv:
                    self.pub_worker.add_to_publish_list(
                        json.dumps(self.html_to_dict_data)
                    )
        return

    def required_data(self, stop_id: int, data: list[str]):
        """Returns only the absolutely necessary data from the html response
        after being manipulated into easily indexed content

        Arguments:
            stop_id -- The id the event took place on
            data -- Each of the indices required from the indexed content
            that have absolutely necessary data.

        Returns:
            A list of this data
        """
        return [stop_id, int(data[3]), int(data[4]), data[5]]

    def html_to_json_like(self, soup: BeautifulSoup):
        """Converts an html BeautifulSoup object into dictionary data to be used
        to publish in the future and wrote to disk.

        Arguments:
            soup -- bs4 souped object from the api response content.
        """
        rows_data = []  # Used in the for loop to capture specific data

        # Gather all the possible h2 containing stop IDs
        stop_ids = [
            re.search("\d+", res.text).group(0)
            for res in soup.find_all(name="h2")
        ]

        # Prep step: find all the tables in the soup object
        tables: list[Tag] = [res for res in soup.find_all(name="table")]

        # Find all the rows in each table, ignoring the first which contains the
        # column info
        tables_data: list[list[Tag]] = [
            BeautifulSoup(table.decode(), "html.parser").find_all(name="tr")[1:]
            for table in tables
        ]

        # First must have one for, for each of the tables we're accessing
        for i in range(len(tables_data)):
            # Then another to traverse through each row of that table
            for j in range(len(tables_data[i])):
                # We appened a new list of required data from the row that we
                # need. Convieniently enough the number of tables and number of
                # stop id's align so there's no guessing which stop id goes to
                # which new list being made.
                rows_data.append(
                    self.required_data(
                        int(stop_ids[i]),
                        tables_data[i][j].get_text(separator=",").split(","),
                    )
                )

        # Make an array of keys needed for the dictionary being built
        keys = ["trip_id", "route_id", "direction", "service_key"]

        # Using the dict constructor, zip, and some list comprehension, it is
        # possible to build a dict entry in this json like object very quickly
        # mapping each key to the exact value in the list created in the for
        # loops above.
        self.html_to_dict_data = [dict(zip(keys, row)) for row in rows_data]

        # Take this dict data and minimize what needs to be published by...
        df = pd.DataFrame().from_dict(self.html_to_dict_data)
        # ...removing all duplicate rows! We don't need all the data, just the
        # stop id and the first rows route, direction and service key
        df_unique = df.drop_duplicates()

        # send it back to a dictionary and format it based on the records
        # property being used so that every entry in the dictionary is aligned
        # to key/value pairs for each row.
        self.html_to_dict_data = df_unique.to_dict(orient="records")
        return

    def save_to_json(self, vehicleID: str) -> None:
        # Build the filename string
        file_str = vehicleID + "-" + mdy_string() + ".json"

        # Build the filepath
        full_file_path = os.path.join(STOPID_DATA_PATH, file_str)

        # Take it from the dict and use json.dumps to turn it into a formatted
        # string.
        json_got = json.dumps(self.html_to_dict_data, indent=4)

        # Write to disk.
        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)
        return

    def data_grabber_main(self) -> None:
        # This function acts as the staging area for independent and combined
        # running with the pipethree.py app.
        os.makedirs(STOPID_DATA_PATH, exist_ok=True)
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")
        self.gather_data(csv_frame)
        log_and_print(message="Gathering complete.")
        return

    def conversion_save_to_json(
        self, save_path: str, mdy_str: str, vehicleID: str
    ) -> None:
        """Using conversion logic path, save modified json to disk

        Arguments:
            save_path -- path being used different from normal logic, which
            is the directory the html is being modified in. This is provided
            through the conversion logic.
            mdy_str -- String provided from conversion logic
            vehicleID -- String provided from conversion logic
        """
        file_str = vehicleID + "-" + mdy_str + ".json"
        full_file_path = os.path.join(save_path, file_str)
        json_got = json.dumps(self.html_to_dict_data, indent=4)

        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)
        return

    def conversion_path(self):
        """Converts all HTML files in the default storage directory to json. If
        a directory only contains json, then it is ignored as this function
        throws away json files from the list and does work for .html files
        """
        # Only do work if the folder exists
        if os.path.exists(STOPID_DATA_FOLDER):

            # List all the folders
            folder_list = os.listdir(STOPID_DATA_FOLDER)
            folder_list.sort()  # Sort them

            # For every folder in the folder list...
            for folder in folder_list:
                # Build the first stage of the path
                files_path = os.path.join(STOPID_DATA_FOLDER, folder)

                # Get the list of files in the file_path just built
                file_list = os.listdir(files_path)

                # Remove everything that's .json
                file_list = [file for file in file_list if ".json" not in file]

                # Sort the .html files by name
                file_list.sort()
                for file in file_list:
                    # now for each file we've identified...
                    # Get the vehicle id (fixed string size allows this!)
                    vehicleID = file[:4]
                    # Same with the mdy in the string!
                    cur_file_mdy = file[5:15]
                    # We need these both when re-writing to a new file and some
                    # other extra operations.

                    # Build another file path to the exact file we are changing
                    file_opened = os.path.join(STOPID_DATA_FOLDER, folder, file)
                    log_and_print(
                        f"Converting {folder}/{file} to .json", prend="\r"
                    )
                    # Read that file in as bytes
                    with open(file_opened, "rb") as infile:
                        # Soup it
                        soup: BeautifulSoup = BeautifulSoup(
                            infile, "html.parser"
                        )
                        # Use that souped object to transform it (same as non
                        # conversion path)
                        self.html_to_json_like(soup=soup)

                        # Save the converted file with all the variables found
                        # earlier and to the file_path we also found earlier.
                        self.conversion_save_to_json(
                            files_path, cur_file_mdy, vehicleID=vehicleID
                        )
                    # Reset the html_to_dict_data so that we work from a clean
                    # slate on the next file
                    self.html_to_dict_data = None


if __name__ == "__main__":
    # This nameguard allows the collector to run entirely independent and not
    # publish the data, just write to disk. This is to assist in the modularity
    # of the pipeline throughout.
    os.makedirs("logs", exist_ok=True)

    grabber = DataGrabberSID(pub_worker=None)
    ans = input(
        "\nWould you like to convert all existing folders from html to json? Y/N: "
    )
    if ans and ans[0].lower() == "y":
        logging.basicConfig(
            format="",
            filename=f"logs/STOPID_CONV-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.INFO,
        )
        log_and_print(message="Conversion path chosen.")
        time.sleep(1)
        grabber.conversion_path()
        log_and_print(message="")
        log_and_print(message="Conversion complete!")
        sys.exit()

    else:
        logging.basicConfig(
            format="",
            filename=f"logs/SID-GRAB-{DATA_MONTH_DAY}.log",
            encoding="utf-8",
            filemode="a",
            level=logging.INFO,
        )
        log_and_print(message="Gathering staring.")
        time.sleep(1)
        grabber.data_grabber_main()
