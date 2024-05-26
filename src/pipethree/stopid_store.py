import io
import logging, os, sys, json
import pandas as pd
import psycopg2

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.pipethree.stopid_transform import SIDDataTransformer
from src.utils.utils import (
    DATA_MONTH_DAY,
    SID_SUBSCRIBER_FOLDER,
    log_and_print,
)

DBNAME = "postgres"
DBUSER = "postgres"
DBPWD = os.getenv("SQL_PW")
BCTABLE = "breadcrumb"
TRIPTABLE = "trip"

# This script can absolutely be ran by itself, but you must ensure that the
# breadcrumb subscriber has finished its work of adding its own data to the
# db before this one goes in to change/update the trip table.


class SIDDataToSQLDB:
    # Saves the stop id data to the postgres SQL db ONLY AFTER the breadcrumb
    # has done its work to write allllll of its information to the db.
    # THIS CANNOT TAKE PLACE BEFORE THAT WORK IS DONE.

    def __init__(self, sidData: list[dict]) -> None:
        """Builds the SIDDataToSQLDB object using information from the
        StopDataRcvr

        Arguments:
            sidData -- list of dict data from the StopDataRcvr
        """
        self.sid_json_data: list[dict] = sidData

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Transform the dataframe before using it to update the sqldb
        preped_df = SIDDataTransformer(df).df_transform()
        return preped_df

    def db_connect(self):
        """Creates a connection to the sql DB on the local host."""
        connection = psycopg2.connect(
            host="localhost",
            database=DBNAME,
            user=DBUSER,
            password=DBPWD,
        )
        connection.autocommit = True
        return connection

    def write_to_db(self, sid_df: pd.DataFrame):
        """Writes the stop id dataframe passed in to the sql database

        Arguments:
            sid_df -- transformed dataframe with data to update in the sql db
        """
        # Track how many rows there are to update in the sql data base
        stop_id_count = sid_df.shape[0]

        # Create the connection
        conn = self.db_connect()

        # Create the connections cursor to execute queries
        cur = conn.cursor()

        # Change transformed df to a dictionary based on the key/value pair from
        # the df columns to the values in the rows
        dict_to_write = sid_df.to_dict(orient="records")

        log_and_print(f"Writing {stop_id_count} rows to Trip table...")

        for record in dict_to_write:
            day_type = ""
            # Change the 'service_key' data in the dictionary to reflect what is
            # allowed in the sql database
            if record["service_key"] == "W" or record["service_key"] == "M":
                day_type = "Weekday"
            elif record["service_key"] == "S":
                day_type = "Saturday"
            elif record["service_key"] == "U":
                day_type = "Sunday"
            else:
                day_type = "Weekday"

            # Change the direction type for each record in the dict
            # to a string that the sql db will use
            direction_type = "Out" if record["direction"] == 0 else "Back"

            # For all the records and their trip_id that line up with this
            # dictionary, update the trip tables missing info from
            # BreadCrumbRcvr
            cur.execute(
                f"UPDATE trip SET route_id={record['route_id']}, "
                + f"service_key='{day_type}', "
                + f"direction='{direction_type}' "
                + f"WHERE trip_id={record['trip_id']};"
            )

        log_and_print(f"Writing {stop_id_count} rows to Trip table COMPLETE!")

    def to_db_start(self):
        """kicks off the storage to the sqldb"""
        sid_df: pd.DataFrame = self.prepare_df(
            pd.DataFrame.from_dict(self.sid_json_data)
        )
        self.write_to_db(sid_df)


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/BC-DBTRANSFERLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    log_and_print("Sending data to SQL database.")
    files = []
    if os.path.exists(SID_SUBSCRIBER_FOLDER):
        files = os.listdir(SID_SUBSCRIBER_FOLDER)
        files.sort()

        while len(files) > 0:
            file = files.pop()
            logging.info("\n")
            log_and_print(f"Next file to transform in memory: {file}")
            curr_file = open(os.path.join(SID_SUBSCRIBER_FOLDER, file))
            json_from_file = json.load(curr_file)
            db_worker = SIDDataToSQLDB(json_from_file)
            db_worker.to_db_start()

    else:
        log_and_print(
            f"Folder {SID_SUBSCRIBER_FOLDER} does not exist. "
            + "Unable to send any data as it does not exist."
        )
        sys.exit(0)
    log_and_print("Data transfer to SQL database complete!")
    sys.exit()
