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


class DataTransformer:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df: pd.DataFrame = self.df_setup(df)
        self.vbd: ValidateBusData = ValidateBusData(self.df)

    def df_setup(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["VEHICLE_ID", "ACT_TIME"], ascending=True)
        df = df.dropna()
        df = df.reset_index(level=0)
        df = df.drop(columns=["index", "GPS_SATELLITES", "GPS_HDOP"])
        return df

    def opd_to_seconds(self, date_format):
        return pd.to_datetime(
            pd.to_datetime(self.df["OPD_DATE"], format=date_format), unit="s"
        )

    def set_timedelta(self) -> pd.Timedelta:
        return pd.to_timedelta(self.df["ACT_TIME"], unit="sec")

    def add_timestamps(self):
        logging.info("\n")
        log_and_print(f"Adding timestamps...")

        self.df.insert(5, "TIMESTAMP", 0)
        date_format = "%d%b%Y:%H:%M:%S"
        opd_sec: pd.Timestamp = self.opd_to_seconds(date_format=date_format)
        td: pd.Timedelta = self.set_timedelta()
        self.df["TIMESTAMP"] = opd_sec + td
        self.df.drop(columns=["OPD_DATE", "ACT_TIME"], inplace=True)

        log_and_print(f"Timestamps complete!")

    def add_speed(self):
        logging.info("\n")
        log_and_print(f"Adding bus speeds in meters/second...")

        self.df.insert(4, "SPEED", 0)
        self.df.insert(4, "dMETERS", self.df["METERS"].diff())
        self.df.insert(4, "dTIMESTAMP", self.df["TIMESTAMP"].diff())
        self.df["SPEED"] = self.df.apply(
            (lambda df: df["dMETERS"] / df["dTIMESTAMP"].total_seconds()),
            axis=1,
        )
        self.df.drop(columns=["dTIMESTAMP", "dMETERS"], inplace=True)

        vid_list = self.df["VEHICLE_ID"].drop_duplicates(keep="first").tolist()

        modified_index = 0
        for id in vid_list:
            curr_data = self.df[self.df["VEHICLE_ID"] == id]
            curr_data.loc[modified_index, "SPEED"] = curr_data.loc[
                modified_index + 1, "SPEED"
            ]
            self.df[self.df["VEHICLE_ID"] == id] = curr_data
            modified_index += curr_data.shape[0]

        log_and_print(f"Bus speed additions complete!")

    def transform_run(self):
        self.add_timestamps()
        self.add_speed()
        self.df = self.vbd.assert_speed_limit()
        return self.df


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/TRANSFORMLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    log_and_print(f"Starting data transformation.")
    files = os.listdir(BC_SUBSCRIBER_FOLDER)
    files.sort()
    for file in files:
        df = pd.read_json(os.path.join(BC_SUBSCRIBER_FOLDER, file))
        transformer = DataTransformer(df)
        logging.info("\n")
        log_and_print(f"Next file to transform in memory: {file}")
        transformer.transform_run()
    log_and_print(f"Data transformation complete.")

    sys.exit()
