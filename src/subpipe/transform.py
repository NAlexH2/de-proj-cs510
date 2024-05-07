import logging, os, sys, json
import pandas as pd
from pathlib import Path

from subpipe.validate import ValidateBusData

if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.utils.utils import (
    DATA_MONTH_DAY,
    SUBSCRIBER_DATA_PATH_JSON,
    SUBSCRIBER_FOLDER,
    curr_time_micro,
    sub_logger,
)


def transform_setup(df: pd.DataFrame):
    new_df = df.sort_values(["VEHICLE_ID", "ACT_TIME"], ascending=True)
    # new_df = new_df.loc[(new_df["VEHICLE_ID"] == 2917)]
    new_df2 = df.sort_values(["VEHICLE_ID", "ACT_TIME"], ascending=True).loc[
        (df["VEHICLE_ID"] == 4043)
    ]
    print("test")


if __name__ == "__main__":
    df = pd.read_json(os.path.join(SUBSCRIBER_FOLDER, "04-11.json"))
    vbd = ValidateBusData(df)

    transform_setup(df)
