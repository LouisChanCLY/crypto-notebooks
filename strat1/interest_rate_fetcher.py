from abc import ABC, abstractmethod
from datetime import UTC, datetime, timedelta
import json
from typing import List
import pandas as pd
import requests
from tqdm import tqdm
from data_fetcher import DataFetcher

from utils import break_date_range_into_intervals


class BinanceInterestRateFetcher(DataFetcher):
    SIZE_LIMIT = 90
    DOWNLOAD_FILE_NAME = (
        "binance_interest_rate_history_{download_time:%Y%m%d%H%M%S}.csv"
    )
    ENDPOINT_URL = "https://www.binance.com/bapi/margin/v1/public/margin/vip/spec/history-interest-rate"

    def __init__(self):
        pass

    def _get_history(
        self, asset: str, start_time: datetime, end_time: datetime, **kwargs
    ) -> pd.Series:
        """Get historical interest rates"""
        response = json.loads(
            requests.get(
                self.ENDPOINT_URL,
                params={
                    "asset": asset,
                    "vipLevel": kwargs.get("vipLevel", 0),
                    "size": self.SIZE_LIMIT,
                    "startTime": int(
                        start_time.timestamp() * 1000
                    ),  # Convert to timestamp in ms
                    "endTime": int(
                        end_time.timestamp() * 1000
                    ),  # Convert to timestamp in ms
                }
                | kwargs,
            ).content
        )
        _ = pd.DataFrame(response["data"])
        _["dailyInterestRate"] = pd.to_numeric(_["dailyInterestRate"])
        _["datetime"] = _["timestamp"].apply(
            lambda x: datetime.fromtimestamp(int(x) / 1000, UTC)
        )
        return (
            _.drop_duplicates()
            .set_index("datetime")
            .sort_index()["dailyInterestRate"]
            .rename(asset)
        )

    def get_history(
        self,
        asset: str | List[str],
        start_time: datetime,
        end_time: datetime,
        save: bool = False,
        **kwargs,
    ) -> pd.Series:
        if isinstance(asset, str):
            asset = [asset]

        timeline = pd.date_range(start=start_time, end=end_time, freq="H", tz="UTC")

        _df = []
        for a in tqdm(asset, desc="Getting Interest Rate History"):
            _ = []
            for start, end in break_date_range_into_intervals(
                start_time=start_time,
                end_time=end_time,
                interval_size=self.SIZE_LIMIT,
                interval_unit="hours",
            ):
                _.append(
                    self._get_history(
                        asset=a,
                        start_time=start,
                        end_time=end,
                        **kwargs,
                    )
                )

            _ = pd.concat(_).sort_index()
            _df.append(_[~_.index.duplicated(keep="first")].reindex(timeline).ffill())

        _df = pd.concat(_df, axis=1)
        _df.index.name = "Date"

        if save:
            print("Saving downloaded data...")
            self.save_file(_df)

        return _df


class OKXInterestRateFetcher(DataFetcher):
    SIZE_LIMIT = 100
    DOWNLOAD_FILE_NAME = "okx_interest_rate_history_{download_time:%Y%m%d%H%M%S}.csv"
    ENDPOINT_URL = "https://www.okx.com/api/v5/finance/savings/lending-rate-history"

    def __init__(self):
        pass

    def _get_history(
        self, asset: str, start_time: datetime, end_time: datetime, **kwargs
    ) -> pd.Series:
        """Get historical interest rates"""
        response = json.loads(
            requests.get(
                self.ENDPOINT_URL,
                params={
                    "ccy": asset,
                    "limit": self.SIZE_LIMIT,
                    "before": int(start_time.timestamp() * 1000),
                    "after": int(end_time.timestamp() * 1000),
                }
                | kwargs,
            ).content
        )
        _ = pd.DataFrame(response["data"])
        _ = _.rename(
            {
                "ccy": "asset",
                "ts": "timestamp",
            },
            axis=1,
        )
        _["rate"] = pd.to_numeric(_["rate"])
        _["datetime"] = _["timestamp"].apply(
            lambda x: datetime.fromtimestamp(int(x) / 1000, UTC)
        )

        return (
            _.set_index("datetime").sort_index().drop_duplicates()["rate"].rename(asset)
        )

    def get_history(
        self,
        asset: str | List[str],
        start_time: datetime,
        end_time: datetime,
        save: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        if isinstance(asset, str):
            asset = [asset]

        timeline = pd.date_range(start=start_time, end=end_time, freq="H", tz="UTC")

        _df = []
        for a in tqdm(asset, desc="Getting Interest Rate History"):
            _ = []
            for start, end in break_date_range_into_intervals(
                start_time=start_time,
                end_time=end_time,
                interval_size=self.SIZE_LIMIT,
                interval_unit="hours",
            ):
                _.append(
                    self._get_history(
                        asset=a,
                        start_time=start + timedelta(hours=-1),  # OKX specific offset
                        end_time=end,
                        **kwargs,
                    )
                )

            _ = pd.concat(_).sort_index()
            _df.append(_[~_.index.duplicated(keep="first")].reindex(timeline).ffill())

        _df = pd.concat(_df, axis=1)
        _df.index.name = "Date"

        if save:
            print("Saving downloaded data...")
            self.save_file(_df)

        return _df
