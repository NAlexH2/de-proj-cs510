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
        longitude_low_bool = longitude_lowest_min > -124  # -124 and more
        longitude_high_bool = longitude_highest_max <= -122  # -122 or less
        try:
            result = (longitude_low_bool) & (longitude_high_bool)
            assert result.all() == True
        except:
            sub_logger(
                f"{curr_time_micro()} LONGITUDE BAD!!!!! Longitude had the following min and max values: "
                + f"{longitude_lowest_min}, {longitude_highest_max}."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} LONGITUDE GOOD! Longitude sits within -122 and -124! "
                + f"Min and max vals are: {longitude_lowest_min}, {longitude_highest_max}."
            )

    def assert_lat_vals(self):
        latitude_lowest_min = self.df["GPS_LATITUDE"].min(axis=0)
        latitude_highest_max = self.df["GPS_LATITUDE"].max(axis=0)
        latitude_low_bool = latitude_lowest_min >= 45  # 45 or more
        latitude_high_bool = latitude_highest_max < 46  # 46 and less
        try:
            result = (latitude_low_bool) & (latitude_high_bool)
            assert result.all() == True
        except:
            sub_logger(
                f"{curr_time_micro()} LATITUDE ASSERT BAD!!!!! Latitude had the following "
                + f"min and max values: {latitude_lowest_min}, {latitude_highest_max}."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} LATITUDE ASSERT! Latitude sits within 45 and 46! Min "
                + f"and max vals are: {latitude_lowest_min}, {latitude_highest_max}."
            )

    def assert_bad_gps_hdop_data(self):
        gathered_HDOPs = self.df[
            (df["GPS_HDOP"] >= 4) & (df["GPS_HDOP"] < 23.1)
        ]
        these_HDOPs_nan = (gathered_HDOPs["GPS_LONGITUDE"].isna()) & (
            gathered_HDOPs["GPS_LATITUDE"].isna()
        )
        try:
            assert these_HDOPs_nan.all() == True
        except:
            sub_logger(
                f"{curr_time_micro()} HDOP ASSERT BAD!!!!! There were some HDOPs with "
                + f"non-nan values on lat and long."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} HDOP ASSERT GOOD! All HDOP values 4 upto "
                + f"(not including) 23.1 are NaN on lat and long."
            )

    def assert_gps_min(self):
        sat_min = self.df["GPS_SATELLITES"].min()
        try:
            assert sat_min == 0
        except:
            sub_logger(
                f"{curr_time_micro()} MIN GPS satellites BAD!!!!! The minimum "
                + f"number of satellites were: {sat_min}"
            )
        else:
            sub_logger(
                f"{curr_time_micro()} MIN GPS satellites GOOD! Minimum number "
                + f"of satellites was: {sat_min}!"
            )

    def assert_gps_max(self):
        sat_max = self.df["GPS_SATELLITES"].max()
        try:
            assert sat_max == 12
        except:
            sub_logger(
                f"{curr_time_micro()} MAX GPS satellites BAD!!!!! The minimum "
                + f"number of satellites were: {sat_max}"
            )
        else:
            sub_logger(
                f"{curr_time_micro()} MAX GPS satellites GOOD! Minimum number "
                + f"of satellites was: {sat_max}!"
            )

    def assert_zero_sat_ok(self):
        zero_sat_df = self.df[df["GPS_SATELLITES"] == 0]
        not_all_nan_lat = zero_sat_df[zero_sat_df["GPS_LATITUDE"].notna()]
        not_all_nan_long = zero_sat_df[zero_sat_df["GPS_LONGITUDE"].notna()]
        try:
            assert not_all_nan_lat["GPS_LATITUDE"].notna().any() == True
            assert not_all_nan_long["GPS_LONGITUDE"].notna().any() == True
        except:
            sub_logger(
                f"{curr_time_micro()} ZERO SATELLITES ASSERT BAD!!!! It seems that all "
                + f"0 GPS satellite vehicles are missing lat and long."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} ZERO SATELLITES ASSERT GOOD! It seems that SOME "
                + f"0 GPS satellite vehicles HAVE a lat and long."
            )

    def assert_twelve_sat_yes(self):
        twelve_sat_df = self.df[df["GPS_SATELLITES"] == 12]
        all_yes_lat = twelve_sat_df[twelve_sat_df["GPS_LATITUDE"].notna()]
        all_yes_long = twelve_sat_df[twelve_sat_df["GPS_LONGITUDE"].notna()]
        try:
            assert all_yes_lat["GPS_LATITUDE"].notna().all() == True
            assert all_yes_long["GPS_LONGITUDE"].notna().all() == True
        except:
            sub_logger(
                f"{curr_time_micro()} TWELVE SATELLITES ASSERT BAD!!!! It seems that some "
                + f"12 GPS satellite vehicles are missing lat and long."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} TWELVE SATELLITES ASSERT GOOD! It seems that ALL "
                + f"12 GPS satellite vehicles HAVE a lat and long."
            )

    def assert_meters_entry(self):
        meters_bool = self.df["ACT_TIME"].isna().all() == False
        try:
            assert meters_bool == True
        except:
            sub_logger(
                f"{curr_time_micro()} ACTIVITY RECORD ASSERT BAD!!!! It seems that some "
                + f"records are missing an event activity time."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} ACTIVITY RECORD ASSERT GOOD! It seems that ALL "
                + f"records HAVE an event activity time."
            )

    def assert_event_act_time(self):
        meters_bool = df["METERS"].isna().all() == False
        try:
            assert meters_bool == True
        except:
            sub_logger(
                f"{curr_time_micro()} METERS RECORD ASSERT BAD!!!! It seems that some "
                + f"records are missing an a meters metric."
            )
        else:
            sub_logger(
                f"{curr_time_micro()} METERS RECORD ASSERT GOOD! It seems that ALL "
                + f"records HAVE an a meters metric."
            )

    def assert_speed_limit(self):
        pass

    def start_assertions(self):
        self.assert_long_vals()
        self.assert_lat_vals()
        self.assert_bad_gps_hdop_data()
        self.assert_gps_min()
        self.assert_gps_max()
        self.assert_zero_sat_ok()  # some 0 gps satellites still have a lat/long
        self.assert_twelve_sat_yes()  # if 12 gps satellites, there must be lat/long
        self.assert_event_act_time()  # Every record has an activity time
        self.assert_meters_entry()  # Every record has a meters entry


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/ASSERTLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    files = os.listdir(SUBSCRIBER_FOLDER)
    files.sort()
    for file in files:
        sub_logger(f"\n{curr_time_micro()} next file {file}")
        df = pd.read_json(os.path.join(SUBSCRIBER_FOLDER, file))
        vbd = ValidateBusData(df)
        vbd.start_assertions()

    sys.exit()
