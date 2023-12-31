from abc import ABC, abstractmethod
from datetime import datetime

import pandas as pd


class DataFetcher(ABC):
    SIZE_LIMIT: int = 0
    DOWNLOAD_FILE_NAME: str = ""

    @abstractmethod
    def _get_history(**kwargs) -> pd.Series | pd.DataFrame:
        ...

    @abstractmethod
    def get_history(**kwargs) -> pd.DataFrame:
        ...

    def save_file(self, df: pd.DataFrame) -> None:
        df.to_csv(
            self.DOWNLOAD_FILE_NAME.format(download_time=datetime.now()), index=True
        )
