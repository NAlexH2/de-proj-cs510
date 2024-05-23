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


class SIDDataTransformer:
    def __init__(self, df: pd.DataFrame) -> None:
        self.df: pd.DataFrame = df

    def df_transform(self) -> pd.DataFrame:
        df_sort = self.df.sort_values(by=self.df.columns[0])
        proper_df = df_sort[df_sort["trip_id"] > 1]
        proper_df = proper_df.reset_index().drop(columns="index")
        proper_df.loc[
            proper_df["service_key"] == "nan", "service_key"
        ] == np.nan
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
