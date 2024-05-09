from pathlib import Path
import logging, os, sys


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


class DataToSQLDB:
    def __init__(self) -> None:
        pass

    def to_db_start(self):
        pass


if __name__ == "__main__":
    pass
