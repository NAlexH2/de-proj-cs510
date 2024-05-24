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


class SIDDataToSQLDB:
    def __init__(self, sidData: list[dict]) -> None:
        self.sid_json_data: list[dict] = sidData

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        preped_df = SIDDataTransformer(df).df_transform()
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

    def write_to_db(self, sid_df: pd.DataFrame):
        stop_id_count = sid_df.shape[0]
        conn = self.db_connect()
        cur = conn.cursor()
        dict_to_write = sid_df.to_dict(orient="records")

        log_and_print(f"Writing {stop_id_count} rows to Trip table...")
        for record in dict_to_write:
            day_type = ""
            if record["service_key"] == "W" or record["service_key"] == "M":
                day_type = "Weekday"
            elif record["service_key"] == "S":
                day_type = "Saturday"
            elif record["service_key"] == "U":
                day_type = "Sunday"
            else:
                day_type = "Weekday"
            direction_type = "Out" if record["direction"] == 0 else "Back"

            cur.execute(
                f"UPDATE trip SET route_id={record['route_id']}, "
                + f"service_key='{day_type}', "
                + f"direction='{direction_type}' "
                + f"WHERE trip_id={record['trip_id']};"
            )

        log_and_print(f"Writing {stop_id_count} rows to Trip table COMPLETE!")

    def to_db_start(self):
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
