import logging, os, sys
import numpy as np
import pandas as pd
from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    DATA_MONTH_DAY,
    SID_SUBSCRIBER_FOLDER,
    log_and_print,
)

# This file can be ran by itself for stop ID data, but is only able to be done
# to closer analyze the results produced by the code and doesn't serve a
# purpose without being ran in conjunction with the stopid_store code.


class SIDDataTransformer:
    """Class transforms the data rcvd from the StopDataRcvr subscriber to fit
    into the shape for the sql database created
    """

    def __init__(self, df: pd.DataFrame) -> None:
        """DF passed in from the json responses received from the subscriber

        Arguments:
            df -- pandas dataframe loaded up from json response from subscriber
        """
        self.df: pd.DataFrame = df

    def df_transform(self) -> pd.DataFrame:
        """Removes and stop id data with a trip_id <= 1, as well as filling in
        any missing service_key data with df.ffill()

        Returns:
            New and transformed dataframe from what was used.
        """
        # Sort and make a copy of the df by the actual event_id
        df_sort = self.df.sort_values(by=self.df.columns[0])

        # Remove any stop ID that is 1 or less. Some of the data is malformed
        # and comes across as -1 or 1 without any more digits present.
        proper_df = df_sort[df_sort["trip_id"] > 1]

        # Drop the new useless index column after resetting the index
        proper_df = proper_df.reset_index().drop(columns="index")

        # Change where the string "nan" appears in the df service_key column
        # to a proper NaN for pandas dataframes by using numpy.nan
        proper_df.loc[
            proper_df["service_key"] == "nan", "service_key"
        ] == np.nan

        # Because ffill or bfill only works on inferring np.nan data, this
        # can now infer the service key based on one that may already exist else
        # where prior to where the NaN showed up.
        proper_df["service_key"] = proper_df["service_key"].ffill()
        return proper_df


if __name__ == "__main__":
    logging.basicConfig(
        format="",
        filename=f"logs/SID-TRANSFORMLOG-{DATA_MONTH_DAY}.log",
        encoding="utf-8",
        filemode="a",
        level=logging.INFO,
    )
    log_and_print(f"Starting data transformation.")
    files = os.listdir(SID_SUBSCRIBER_FOLDER)
    files.sort()
    for file in files:
        df = pd.read_json(os.path.join(SID_SUBSCRIBER_FOLDER, file))
        transformer = SIDDataTransformer(df)
        logging.info("\n")
        log_and_print(f"Next file to transform in memory: {file}")
        transformer.df_transform()
    log_and_print(f"Data transformation complete.")

    sys.exit()
