import io
import logging, os, sys, json
import pandas as pd
import psycopg2

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.subpipe.validate import ValidateBusData
from src.subpipe.transform import DataTransformer
from src.utils.utils import (
    DATA_MONTH_DAY,
    BC_SUBSCRIBER_FOLDER,
    log_and_print,
)

DBNAME = "postgres"
DBUSER = "postgres"
DBPWD = os.getenv("SQL_PW")
BCTABLE = "breadcrumb"
TRIPTABLE = "trip"


class BCDataToSQLDB:
    def __init__(self, jData: dict) -> None:
        self.bc_json_data: dict = jData

    def make_breadcrumb_table(self, df: pd.DataFrame) -> pd.DataFrame:
        renamer = {
            "TIMESTAMP": "tstamp",
            "GPS_LATITUDE": "latitude",
            "GPS_LONGITUDE": "longitude",
            "SPEED": "speed",
            "EVENT_NO_TRIP": "trip_id",
        }
        bc_table = df[
            [
                "TIMESTAMP",
                "GPS_LATITUDE",
                "GPS_LONGITUDE",
                "SPEED",
                "EVENT_NO_TRIP",
            ]
        ]
        bc_table = bc_table.rename(columns=renamer)
        return bc_table

    def make_trip_table(self, df: pd.DataFrame) -> pd.DataFrame:
        renamer = {"EVENT_NO_TRIP": "trip_id", "VEHICLE_ID": "vehicle_id"}
        trip_table = df[["EVENT_NO_TRIP", "VEHICLE_ID"]].drop_duplicates(
            keep="first"
        )
        trip_table.insert(1, "route_id", 0)
        trip_table.insert(3, "service_key", "Weekday")
        trip_table.insert(4, "direction", "Out")
        trip_table.rename(columns=renamer, inplace=True)
        return trip_table

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        ValidateBusData(df).do_all_assertions_except_speed()
        preped_df = DataTransformer(df).transform_run()
        return preped_df

    def db_connect(self):
        connection = psycopg2.connect(
            host="localhost",
            database=DBNAME,
            user=DBUSER,
            password=DBPWD,
        )
        connection.autocommit = True
        return connection

    def to_file_like(self, df: pd.DataFrame) -> io.StringIO:
        csv_temp_string = df.to_csv(index=False)
        file_like = io.StringIO(csv_temp_string)
        return file_like

    def write_to_db(self, trip_frame: pd.DataFrame, bc_frame: pd.DataFrame):
        trip_row_count = trip_frame.shape[0]
        bc_row_count = bc_frame.shape[0]
        conn = self.db_connect()
        cur = conn.cursor()
        trip_file_like = self.to_file_like(trip_frame)
        bc_file_like = self.to_file_like(bc_frame)

        log_and_print(f"Writing {trip_row_count} rows to Trip table...")
        with trip_file_like as tf:
            next(tf)
            cur.copy_from(tf, TRIPTABLE, sep=",")

        log_and_print(f"Writing {trip_row_count} rows to Trip table COMPLETE!")

        log_and_print(f"Writing {bc_row_count} rows to BreadCrumb table...")
        with bc_file_like as bcf:
            next(bcf)
            cur.copy_from(bcf, BCTABLE, sep=",")

        log_and_print(
            f"Writing {bc_row_count} rows to BreadCrumb table COMPLETE!"
        )

    def to_db_start(self):
        preped_df: pd.DataFrame = self.prepare_df(
            pd.DataFrame.from_dict(self.bc_json_data)
        )
        bc_table: pd.DataFrame = self.make_breadcrumb_table(preped_df)
        trip_table: pd.DataFrame = self.make_trip_table(preped_df)
        self.write_to_db(trip_table, bc_table)


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
    if os.path.exists(BC_SUBSCRIBER_FOLDER):
        files = os.listdir(BC_SUBSCRIBER_FOLDER)
        files.sort()

        while len(files) > 0:
            file = files.pop()
            logging.info("\n")
            log_and_print(f"Next file to transform in memory: {file}")
            curr_file = open(os.path.join(BC_SUBSCRIBER_FOLDER, file))
            json_from_file = json.load(curr_file)
            db_worker = BCDataToSQLDB(json_from_file)
            db_worker.to_db_start()

    else:
        log_and_print(
            f"Folder {BC_SUBSCRIBER_FOLDER} does not exist. "
            + "Unable to send any data as it does not exist."
        )
        sys.exit(0)
    log_and_print("Data transfer to SQL database complete!")
    sys.exit()
