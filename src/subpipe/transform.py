import logging, os, sys, json
import pandas as pd
from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.subpipe.validate import ValidateBusData
from src.utils.utils import (
    DATA_MONTH_DAY,
    BC_SUBSCRIBER_FOLDER,
    log_and_print,
)

# Just like every other file, this can be ran independently, but does not
# effectively do anything outside of allowing someone to debug and play with
# the transformed data.


class BCDataTransformer:
    """Class transforms the data rcvd from the BreadCrumbRcvr subscriber to
    fit into the shape for the sql database created
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """DF passed in from the json responses received from the subscriber

        Arguments:
            df -- pandas dataframe loaded up from json response from subscriber
        """
        # modify the dataframe in setup to do some preparatory cleanup steps
        self.df: pd.DataFrame = self.df_setup(df)

        # Using this newly shaped df, share it to be used in the validate bus
        # data class, specifically around the speed of vehicles, not to validate
        # all the data. The validation step takes place before transform.
        self.vbd: ValidateBusData = ValidateBusData(self.df)

    def df_setup(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cleans up the dataframe to be used correctly and sorts it accordingly

        Arguments:
            df -- dataframe from constructor to be shaped correctly

        Returns:
            Proper shape dataframe
        """

        # Sort the df by the id and time the events took place so that all vid
        # and their respective times line up perfectly
        df = df.sort_values(["VEHICLE_ID", "ACT_TIME"], ascending=True)

        # Drop any rows with NaN
        df = df.dropna()

        # Reset the index so that we have indexed order want it in.
        df = df.reset_index(level=0)

        # Drop the useless columns at this point. reset_index creates a new col
        # with the old indices, need to drop it too.
        df = df.drop(columns=["index", "GPS_SATELLITES", "GPS_HDOP"])
        return df

    def opd_to_seconds(self, date_format):
        # Turns the opreating date into seconds to support being manipulated
        # to accurately project the date/time the event takes place later
        return pd.to_datetime(
            pd.to_datetime(self.df["OPD_DATE"], format=date_format), unit="s"
        )

    def set_timedelta(self) -> pd.Timedelta:
        # Make/set a timedelta in accordance to the act_time in seconds
        return pd.to_timedelta(self.df["ACT_TIME"], unit="sec")

    def add_timestamps(self):
        # What it says on the tin. Adds timestamps.
        logging.info("\n")
        log_and_print(f"Adding timestamps...")

        # Insert the TIMESTAMP column to the dataframe
        self.df.insert(5, "TIMESTAMP", 0)

        # Specify the dateformat we want used
        date_format = "%d%b%Y:%H:%M:%S"

        # Pass the datetime string in, change the time from what it was to the
        # new format using the seconds from the old format.
        opd_sec: pd.Timestamp = self.opd_to_seconds(date_format=date_format)

        # Set a timedelta to calculate the difference between the operating date
        # and the activity time. Both of these being set to seconds supports
        # simple transformation.
        td: pd.Timedelta = self.set_timedelta()

        # opd_sec is a series which uses the td series to calc the time diff.
        # Add the two things to get the accurate time the event took place on.
        self.df["TIMESTAMP"] = opd_sec + td

        # After this is done, drop the useless columns as we now have accurate
        # timestamps taking place.
        self.df.drop(columns=["OPD_DATE", "ACT_TIME"], inplace=True)

        log_and_print(f"Timestamps complete!")

    def add_speed(self):
        # add speed in meters per second to the dataframe for the class
        logging.info("\n")
        log_and_print(f"Adding bus speeds in meters/second...")

        # Insert 3 columns with specific default values
        self.df.insert(4, "SPEED", 0)
        self.df.insert(4, "dMETERS", self.df["METERS"].diff())
        self.df.insert(4, "dTIMESTAMP", self.df["TIMESTAMP"].diff())

        # Use apply to calculate the speed using the difference between
        # the change in meters the bus has moved the difference in time
        # between each 5 second event
        self.df["SPEED"] = self.df.apply(
            (lambda df: df["dMETERS"] / df["dTIMESTAMP"].total_seconds()),
            axis=1,
        )

        # Drop the newest useless columns
        self.df.drop(columns=["dTIMESTAMP", "dMETERS"], inplace=True)

        # Quickly and simply get a vehicle id list to work with
        vid_list = self.df["VEHICLE_ID"].drop_duplicates(keep="first").tolist()

        modified_index = 0
        # For the first entry for every single vehicle ID it will be 0, this
        # will set it to 2nd record for that vehicle ID speed.
        for id in vid_list:
            curr_data = self.df[self.df["VEHICLE_ID"] == id]
            curr_data.loc[modified_index, "SPEED"] = curr_data.loc[
                modified_index + 1, "SPEED"
            ]
            self.df[self.df["VEHICLE_ID"] == id] = curr_data
            modified_index += curr_data.shape[0]

        log_and_print(f"Bus speed additions complete!")

    def transform_run(self):
        # Does all the work in a specified order neatly for the dataframe
        self.add_timestamps()
        self.add_speed()
        self.df = self.vbd.assert_speed_limit()
        return self.df


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/BC-TRANSFORMLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    log_and_print(f"Starting data transformation.")
    files = os.listdir(BC_SUBSCRIBER_FOLDER)
    files.sort()
    for file in files:
        df = pd.read_json(os.path.join(BC_SUBSCRIBER_FOLDER, file))
        transformer = BCDataTransformer(df)
        logging.info("\n")
        log_and_print(f"Next file to transform in memory: {file}")
        transformer.transform_run()
    log_and_print(f"Data transformation complete.")

    sys.exit()
