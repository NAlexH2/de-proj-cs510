import logging, os, sys, json
import pandas as pd

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.subpipe.validate import ValidateBusData
from src.subpipe.transform import DataTransformer
from src.utils.utils import (
    DATA_MONTH_DAY,
    SUBSCRIBER_FOLDER,
    curr_time_micro,
    sub_logger,
)

"""Here's the plan...
    When operating with subscriber pipeline:
        1. This takes in the collection of strings from subscriber.data_to_write
        2. Turn it into a dict w/ something like....
            json_data = []
            while len(self.data_to_write) > 0:
                data_prep = self.data_to_write.pop()
                json_data.append(json.loads(data_prep))
        3. Take this whole thing and load it into a pd dataframe much like
           how pd.read_json does... some how. Maybe make fake file object
        4. split dataframe into 2 separate dataframes to load into the tables
            individually.
        5. do this by converting the dataframe down back into a csv, then into
            a fake file object, then use psycopg2 `copy_from`
    
    When operating independently:
        1. Most the same, but loading from the files in a for loop first
        2. The rest should be the same ish? We'll see... Get it working with
            subscriber first.
"""
DBNAME = "postgres"
DBUSER = "postgres"
DBPWD = os.getenv("SQL_PW")


class DataToSQLDB:
    def __init__(self, jData: dict) -> None:
        self.json_data: dict = jData

    def make_breadcrumb_table(self, df: pd.DataFrame) -> pd.DataFrame:
        bc_table = df[
            df[
                "TIMESTAMP",
                "GPS_LATITUDE",
                "GPS_LONGITUDE",
                "SPEED",
                "EVENT_NO_TRIP",
            ]
        ]
        return bc_table

    def make_trip_table(self, df: pd.DataFrame) -> pd.DataFrame:
        pass

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        ValidateBusData(df).do_all_assertions_except_speed()
        preped_df = DataTransformer(df).transform_run()
        ValidateBusData(preped_df).assert_speed_limit()
        return preped_df

    def to_db_start(self):
        preped_df = self.prepare_df(pd.DataFrame.from_dict(self.json_data))
        bc_table = self.make_breadcrumb_table(preped_df)
        trip_table = self.make_trip_table(preped_df)


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/DBTRANSFERLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    sub_logger(f"{curr_time_micro()} Sending data to SQL database.")
    files = []
    if os.path.exists(SUBSCRIBER_FOLDER):
        files = os.listdir(SUBSCRIBER_FOLDER)
        files.sort()

        while len(files) > 0:
            file = files.pop()
            sub_logger(
                f"\n{curr_time_micro()} Next file to transform in memory: {file}"
            )
            curr_file = open(os.path.join(SUBSCRIBER_FOLDER, file))
            json_from_file = json.load(curr_file)
            db_worker = DataToSQLDB(json_from_file)
            db_worker.to_db_start()

    else:
        sub_logger(
            f"{curr_time_micro()} Folder {SUBSCRIBER_FOLDER} does not exist. "
            + f"Unable to send any data as it does not exist."
        )
        sys.exit(0)
    sub_logger(f"{curr_time_micro()} Data transfer to SQL database complete!")
    sys.exit()
