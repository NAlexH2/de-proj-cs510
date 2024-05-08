import logging, os, sys, json
import pandas as pd
from pathlib import Path


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).parents[2].absolute()))

from src.subpipe.validate import ValidateBusData
from src.utils.utils import (
    DATA_MONTH_DAY,
    SUBSCRIBER_DATA_PATH_JSON,
    SUBSCRIBER_FOLDER,
    curr_time_micro,
    sub_logger,
)

class DataTransformer:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df: pd.DataFrame = self.df_setup(df)

    def df_setup(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.sort_values(["VEHICLE_ID", "ACT_TIME"], ascending=True)
        df = df.dropna()
        df = df.reset_index(level=0)
        df = df.drop(columns=['index', 'GPS_SATELLITES', 'GPS_HDOP'])
        return df

    def opd_to_seconds(self, date_format):
        return (
            pd.to_datetime(
                pd.to_datetime(self.df['OPD_DATE'], format=date_format), unit='s'
                )
            )
    
    def set_timedelta(self) -> pd.Timedelta:
        return pd.to_timedelta(self.df['ACT_TIME'], unit='sec')
                

    def add_timestamps(self):
        self.df.insert(5, 'TIMESTAMP', 0)
        date_format = "%d%b%Y:%H:%M:%S"
        opd_sec: pd.Timestamp = self.opd_to_seconds(date_format=date_format)
        td: pd.Timedelta = self.set_timedelta()
        self.df['TIMESTAMP'] = opd_sec + td
        self.df.drop(columns=['OPD_DATE'], inplace=True)
        print(self.df.head())


    def transform_run(self):
        self.add_timestamps()
        
        


if __name__ == "__main__":
    transformer = DataTransformer(pd.read_json(os.path.join(SUBSCRIBER_FOLDER, "04-11.json")))
    vbd = ValidateBusData(transformer.df)
    
    transformer.transform_run()

