import io
import logging, os, sys, json
import pandas as pd
import psycopg2

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.subpipe.validate import ValidateBusData
from src.subpipe.transform import BCDataTransformer
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
        """Builds the SIDDataToSQLDB object using information from the
        BreadCrumbRcvr

        Arguments:
            jData -- dict data from the BreadCrumbRcvr
        """
        self.bc_json_data: dict = jData

    def make_breadcrumb_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Makes and renames the columns of the breadcrumb dataframe to
        support simple insertion into the database

        Arguments:
            df -- the dataframe to have names changed

        Returns:
            New dataframe that has only the data required for the breadcrumb
            sql table. Renames the columns to simplify this.
        """
        # Dict to be used to rename the columns for the new dataframe
        renamer = {
            "TIMESTAMP": "tstamp",
            "GPS_LATITUDE": "latitude",
            "GPS_LONGITUDE": "longitude",
            "SPEED": "speed",
            "EVENT_NO_TRIP": "trip_id",
        }

        # Make a new df with specific columns from the original df
        bc_table = df[
            [
                "TIMESTAMP",
                "GPS_LATITUDE",
                "GPS_LONGITUDE",
                "SPEED",
                "EVENT_NO_TRIP",
            ]
        ]

        # Rename the columns in the new df
        bc_table = bc_table.rename(columns=renamer)

        # return new df to be used
        return bc_table

    def make_trip_table(self, df: pd.DataFrame) -> pd.DataFrame:
        """Makes the trip table and renames the columns to simplify insertion
        in the sql db

        Arguments:
            df -- the dataframe that will be used to extract the info for the
            trip table and then renamed.

        Returns:
            New df renamed columns with the data for the trip table to simplify
            insertion into the sql db.
        """
        # Dict to be used to rename the columns for the new dataframe
        renamer = {"EVENT_NO_TRIP": "trip_id", "VEHICLE_ID": "vehicle_id"}

        # Make a new df with specific columns from the original df
        trip_table = df[["EVENT_NO_TRIP", "VEHICLE_ID"]].drop_duplicates(
            keep="first"
        )

        # Insert new columns with defaults required for inserting into the sql
        # trip table
        trip_table.insert(1, "route_id", 0)
        trip_table.insert(3, "service_key", "Weekday")
        trip_table.insert(4, "direction", "Out")
        trip_table.rename(columns=renamer, inplace=True)
        return trip_table

    def prepare_df(self, df: pd.DataFrame) -> pd.DataFrame:
        # Assert the data is exactly what it should be except for the speed
        ValidateBusData(df).do_all_assertions_except_speed()

        # prep the df by running it through the transformer
        preped_df = BCDataTransformer(df).transform_run()
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

    def to_file_like(self, df: pd.DataFrame) -> io.StringIO:
        """Turns a pandas dataframe into a file like object to be used by the
        copy_from to insert into the db. Turns it into a csv then loads the
        CSV into a io.StringIO() object to be read from.

        Arguments:
            df -- df to be used to make a file like object

        Returns:
            a file like object
        """
        csv_temp_string = df.to_csv(index=False)
        file_like = io.StringIO(csv_temp_string)
        return file_like

    def write_to_db(self, trip_frame: pd.DataFrame, bc_frame: pd.DataFrame):
        """Writes the stop id dataframe passed in to the sql database

        Arguments:
            trip_frame -- transformed trip df to be used on insertion to sql db
            bc_frame -- transformed breadcrumb df to be used on insertion to sql db
        """
        # Get the row count for each dataframe
        trip_row_count = trip_frame.shape[0]
        bc_row_count = bc_frame.shape[0]

        # Create the connection
        conn = self.db_connect()

        # Create the connections cursor to execute queries
        cur = conn.cursor()

        # Turn both df into a file like object for psycopg2 to use in it's
        # copy_from
        trip_file_like = self.to_file_like(trip_frame)
        bc_file_like = self.to_file_like(bc_frame)

        log_and_print(f"Writing {trip_row_count} rows to Trip table...")
        with trip_file_like as tf:
            # skip the top of the file where the columns in the csv are
            next(tf)
            # using the file like object, read and load the file into the
            # trip table, and that each thing entry is csv
            cur.copy_from(tf, TRIPTABLE, sep=",")

        log_and_print(f"Writing {trip_row_count} rows to Trip table COMPLETE!")

        log_and_print(f"Writing {bc_row_count} rows to BreadCrumb table...")
        with bc_file_like as bcf:
            # skip the top of the file where the columns in the csv are
            next(bcf)
            # Same process as the with from the trip_table
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
