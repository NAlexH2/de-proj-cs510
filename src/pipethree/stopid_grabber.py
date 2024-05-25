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
    def __init__(self, pub_worker: PipelinePublisherSID) -> None:
        self.pub_worker = 0
        self.html_to_dict_data: list[dict] = None
        self.pub_worker: PipelinePublisherSID = pub_worker
        self.con_path: bool = False

    def gather_data(self, cf: pd.DataFrame):
        for i in range(cf.size):
            vehicleID = str(cf["Snickers"].at[i])
            log_and_print(message=f"VID: {vehicleID}", prend="\r")
            try:
                resp: requests.Response = requests.request(
                    "GET", STOPID_API_URL + vehicleID, timeout=15
                )
            except requests.ConnectTimeout:
                log_and_print(
                    message=f"{vehicleID} request failed\n",
                )
                continue

            soup: BeautifulSoup = BeautifulSoup(resp.content, "html.parser")
            content = soup.decode()

            if resp.status_code != 404:
                self.html_to_json_like(soup)
                self.save_to_json(vehicleID)
                if "-P" in sys.argv:
                    self.pub_worker.add_to_publish_list(
                        json.dumps(self.html_to_dict_data)
                    )
        return

    def required_data(self, stop_id: int, data: list[str]):
        return [stop_id, int(data[3]), int(data[4]), data[5]]

    def html_to_json_like(self, soup: BeautifulSoup):
        stop_ids = [
            re.search("\d+", res.text).group(0)
            for res in soup.find_all(name="h2")
        ]
        tables: list[Tag] = [res for res in soup.find_all(name="table")]

        tables_data: list[list[Tag]] = [
            BeautifulSoup(table.decode(), "html.parser").find_all(name="tr")[1:]
            for table in tables
        ]

        rows_data = []

        for i in range(len(tables_data)):
            for j in range(len(tables_data[i])):
                rows_data.append(
                    self.required_data(
                        int(stop_ids[i]),
                        tables_data[i][j].get_text(separator=",").split(","),
                    )
                )

        keys = ["trip_id", "route_id", "direction", "service_key"]
        self.html_to_dict_data = [dict(zip(keys, row)) for row in rows_data]
        df = pd.DataFrame().from_dict(self.html_to_dict_data)
        df_unique = df.drop_duplicates()
        self.html_to_dict_data = df_unique.to_dict(orient="records")
        return

    def save_to_json(self, vehicleID: str) -> None:
        file_str = vehicleID + "-" + mdy_string() + ".json"
        full_file_path = os.path.join(STOPID_DATA_PATH, file_str)
        json_got = json.dumps(self.html_to_dict_data, indent=4)

        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)
        return

    def data_grabber_main(self) -> None:
        os.makedirs(STOPID_DATA_PATH, exist_ok=True)
        csv_frame: pd.DataFrame = pd.read_csv("./src/vehicle_ids.csv")
        self.gather_data(csv_frame)
        log_and_print(message="Gathering complete.")
        return

    def conversion_save_to_json(
        self, save_path: str, mdy_str: str, vehicleID: str
    ) -> None:
        file_str = vehicleID + "-" + mdy_str + ".json"
        full_file_path = os.path.join(save_path, file_str)
        json_got = json.dumps(self.html_to_dict_data, indent=4)

        with open(full_file_path, "w") as outfile:
            outfile.write(json_got)
        return

    def conversion_path(self):
        self.con_path = True
        if os.path.exists(STOPID_DATA_FOLDER):
            folder_list = os.listdir(STOPID_DATA_FOLDER)
            folder_list.sort()
            for folder in folder_list:
                files_path = os.path.join(STOPID_DATA_FOLDER, folder)
                file_list = os.listdir(files_path)
                file_list = [file for file in file_list if ".json" not in file]
                file_list.sort()
                for file in file_list:
                    vehicleID = file[:4]
                    cur_file_mdy = file[5:15]
                    file_opened = os.path.join(STOPID_DATA_FOLDER, folder, file)
                    log_and_print(
                        f"Converting {folder}/{file} to .json", prend="\r"
                    )
                    with open(file_opened, "rb") as infile:
                        soup: BeautifulSoup = BeautifulSoup(
                            infile, "html.parser"
                        )
                        self.html_to_json_like(soup=soup)
                        self.conversion_save_to_json(
                            files_path, cur_file_mdy, vehicleID=vehicleID
                        )
                    self.html_to_dict_data = None


if __name__ == "__main__":
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
