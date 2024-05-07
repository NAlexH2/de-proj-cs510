import logging, os, sys, json
import pandas as pd

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    DATA_MONTH_DAY,
    SUBSCRIBER_DATA_PATH_JSON,
    SUBSCRIBER_FOLDER,
    curr_time_micro,
    sub_logger,
)


class ValidateBusData:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df: pd.DataFrame = df

    # Assert long is never less than -124, but always greater than -122
    def assert_long_vals(self):
        longitude_lowest_min = self.df["GPS_LONGITUDE"].min(axis=0)
        longitude_highest_max = self.df["GPS_LONGITUDE"].max(axis=0)
        longitude_low_bool = longitude_lowest_min > -124  # -124 or more
        longitude_high_bool = longitude_highest_max < -122  # -122 or less
        try:
            result = (longitude_low_bool) & (longitude_high_bool)
            assert result.all() == True
        except:
            sub_logger(
                f"{curr_time_micro()} BAD!!!!! Longitude had the following min and max values: "
                + f"{longitude_lowest_min}, {longitude_highest_max}."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} GOOD! Longitude sits within -122 and -124! Min and max vals are: "
                + f"{longitude_lowest_min}, {longitude_highest_max}."
            )

    def assert_lat_vals(self):
        pass

    def assert_speed_limit(self):
        pass

    def assert_bad_gps_hdop_data(self):
        pass

    def assert_gps_min(self):
        pass

    def assert_gps_max(self):
        pass

    def assert_zero_sat_ok(self):
        pass

    def assert_twelve_sat_yes(self):
        pass

    def assert_multi_trip_stops(self):
        pass

    def start_assertions(self):
        self.assert_long_vals()
        self.assert_lat_vals()
        self.assert_speed_limit()
        self.assert_bad_gps_hdop_data()
        self.assert_gps_min()
        self.assert_gps_max()
        self.assert_zero_sat_ok()
        self.assert_twelve_sat_yes()  # if 12 gps satellites, there must be lat/long
        self.assert_multi_trip_stops()


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/ASSERTLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    df = pd.read_json(os.path.join(SUBSCRIBER_FOLDER, "04-11.json"))
    vbd = ValidateBusData(df)
    vbd.start_assertions()
    sys.exit()
