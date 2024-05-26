import logging, os, sys
import pandas as pd

from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    DATA_MONTH_DAY,
    BC_SUBSCRIBER_FOLDER,
    log_and_print,
)


class ValidateBusData:
    """Validation class used in the step to validate data or in the case of
    removing erroneous speeds in the transform step.
    """

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
            log_and_print(
                f"LONGITUDE BAD!!!!! Longitude had the following min and max values: "
                + f"{longitude_lowest_min}, {longitude_highest_max}."
            )
            return False
        else:
            log_and_print(
                f"LONGITUDE GOOD! Longitude sits within -122 and -124! "
                + f"Min and max vals are: {longitude_lowest_min}, {longitude_highest_max}."
            )
            return True

    def assert_lat_vals(self):
        latitude_lowest_min = self.df["GPS_LATITUDE"].min(axis=0)
        latitude_highest_max = self.df["GPS_LATITUDE"].max(axis=0)
        latitude_low_bool = latitude_lowest_min >= 45  # 45 or more
        latitude_high_bool = latitude_highest_max < 46  # 46 and less
        try:
            result = (latitude_low_bool) & (latitude_high_bool)
            assert result.all() == True
        except:
            log_and_print(
                f"LATITUDE ASSERT BAD!!!!! Latitude had the following "
                + f"min and max values: {latitude_lowest_min}, {latitude_highest_max}."
            )
            return False
        else:
            log_and_print(
                f"LATITUDE ASSERT! Latitude sits within 45 and 46! Min "
                + f"and max vals are: {latitude_lowest_min}, {latitude_highest_max}."
            )
            return True

    def assert_bad_gps_hdop_data(self):
        gathered_HDOPs = self.df[
            (self.df["GPS_HDOP"] >= 4) & (self.df["GPS_HDOP"] < 23.1)
        ]
        these_HDOPs_nan = (gathered_HDOPs["GPS_LONGITUDE"].isna()) & (
            gathered_HDOPs["GPS_LATITUDE"].isna()
        )
        try:
            assert these_HDOPs_nan.all() == True
        except:
            log_and_print(
                f"HDOP ASSERT BAD!!!!! There were some HDOPs with "
                + f"non-nan values on lat and long."
            )
            return False
        else:
            log_and_print(
                f"HDOP ASSERT GOOD! All HDOP values 4 upto "
                + f"(not including) 23.1 are NaN on lat and long."
            )
            return True

    def assert_gps_min(self):
        sat_min = self.df["GPS_SATELLITES"].min()
        try:
            assert sat_min == 0
        except:
            log_and_print(
                f"MIN GPS satellites BAD!!!!! The minimum "
                + f"number of satellites were: {sat_min}"
            )
            return False
        else:
            log_and_print(
                f"MIN GPS satellites GOOD! Minimum number "
                + f"of satellites was: {sat_min}!"
            )
            return True

    def assert_gps_max(self):
        sat_max = self.df["GPS_SATELLITES"].max()
        try:
            assert sat_max == 12
        except:
            log_and_print(
                f"MAX GPS satellites BAD!!!!! The minimum "
                + f"number of satellites were: {sat_max}"
            )
            return False
        else:
            log_and_print(
                f"MAX GPS satellites GOOD! Minimum number "
                + f"of satellites was: {sat_max}!"
            )
            return True

    def assert_zero_sat_ok(self):
        zero_sat_df = self.df[self.df["GPS_SATELLITES"] == 0]
        not_all_nan_lat = zero_sat_df[zero_sat_df["GPS_LATITUDE"].notna()]
        not_all_nan_long = zero_sat_df[zero_sat_df["GPS_LONGITUDE"].notna()]
        try:
            assert not_all_nan_lat["GPS_LATITUDE"].notna().any() == True
            assert not_all_nan_long["GPS_LONGITUDE"].notna().any() == True
        except:
            log_and_print(
                f"ZERO SATELLITES ASSERT BAD!!!! It seems that all "
                + f"0 GPS satellite vehicles are missing lat and long."
            )
            return False
        else:
            log_and_print(
                f"ZERO SATELLITES ASSERT GOOD! It seems that SOME "
                + f"0 GPS satellite vehicles HAVE a lat and long."
            )
            return True

    def assert_twelve_sat_yes(self):
        twelve_sat_df = self.df[self.df["GPS_SATELLITES"] == 12]
        all_yes_lat = twelve_sat_df[twelve_sat_df["GPS_LATITUDE"].notna()]
        all_yes_long = twelve_sat_df[twelve_sat_df["GPS_LONGITUDE"].notna()]
        try:
            assert all_yes_lat["GPS_LATITUDE"].notna().all() == True
            assert all_yes_long["GPS_LONGITUDE"].notna().all() == True
        except:
            log_and_print(
                f"TWELVE SATELLITES ASSERT BAD!!!! It seems that some "
                + f"12 GPS satellite vehicles are missing lat and long."
            )
            return False
        else:
            log_and_print(
                f"TWELVE SATELLITES ASSERT GOOD! It seems that ALL "
                + f"12 GPS satellite vehicles HAVE a lat and long."
            )
            return True

    def assert_meters_entry(self):
        meters_bool = self.df["ACT_TIME"].isna().all() == False
        try:
            assert meters_bool == True
        except:
            log_and_print(
                f"ACTIVITY RECORD ASSERT BAD!!!! It seems that some "
                + f"records are missing an event activity time."
            )
            return False
        else:
            log_and_print(
                f"ACTIVITY RECORD ASSERT GOOD! It seems that ALL "
                + f"records HAVE an event activity time."
            )
            return True

    def assert_event_act_time(self):
        meters_bool = self.df["METERS"].isna().all() == False
        try:
            assert meters_bool == True
        except:
            log_and_print(
                f"METERS RECORD ASSERT BAD!!!! It seems that some "
                + f"records are missing an a meters metric."
            )
            return False
        else:
            log_and_print(
                f"METERS RECORD ASSERT GOOD! It seems that ALL "
                + f"records HAVE an a meters metric."
            )
            return True

    def assert_speed_limit(self):
        kmh_data = pd.DataFrame()
        kmh_data.insert(0, "SPEED", value=(self.df["SPEED"] * 3.6))
        try:
            assert (kmh_data["SPEED"] < 113).all() == True
        except:
            log_and_print(
                f"SPEED ASSERTION ALERT!!! Some speeds on "
                + f"some buses appears to be over 113KMH/70MPH. This is not possible, "
                + f"is an extreme anomaly, and will be removed from the dataset."
            )
            ids_over_limit = self.df.loc[
                self.df["SPEED"] > (113 / 3.6), "VEHICLE_ID"
            ].unique()
            log_and_print(
                f"Following vehicles have speeds greater "
                + f"than 113KMH/70MPH: {ids_over_limit}"
            )
            return self.df[self.df["SPEED"] < (113 / 3.6)]
        else:
            log_and_print(
                f"SPEED LIMIT ASSERT GOOD! It seems that no "
                + f"bus speeds are over 113KMH/70MPH!"
            )
            return self.df

    def do_all_assertions_except_speed(self):
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
        filename=f"logs/BC-ASSERTLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    files = os.listdir(BC_SUBSCRIBER_FOLDER)
    files.sort()
    for file in files:
        logging.info("\n")
        log_and_print(f"Next file to assert: {file}")
        df = pd.read_json(os.path.join(BC_SUBSCRIBER_FOLDER, file))
        vbd = ValidateBusData(df)
        vbd.do_all_assertions()

    sys.exit()
