from datetime import UTC, datetime, timedelta
from typing import List
from binance.spot import Spot
import pandas as pd
from tqdm import tqdm
from data_fetcher import DataFetcher

from utils import break_date_range_into_intervals


class BinancePriceFetcher(DataFetcher):
    SIZE_LIMIT = 1000
    DOWNLOAD_FILE_NAME = "binance_price_history_{download_time:%Y%m%d%H%M%S}.csv"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self._client = Spot(api_key, api_secret)

    def save_file(self, df: pd.DataFrame) -> None:
        df.to_csv(
            self.DOWNLOAD_FILE_NAME.format(download_time=datetime.now()), index=True
        )

    def _get_history(
        self,
        symbol: str,
        interval: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> pd.DataFrame:
        _ = pd.DataFrame(
            self._client.klines(
                symbol=symbol,
                interval=interval,
                startTime=int(
                    start_time.timestamp() * 1000
                ),  # Convert to timestamp in ms
                endTime=int(end_time.timestamp() * 1000),  # Convert to timestamp in ms
                limit=self.SIZE_LIMIT,
                **kwargs,
            ),
            columns=[
                "date",
                f"{symbol}_open",
                f"{symbol}_high",
                f"{symbol}_low",
                f"{symbol}_close",
                f"{symbol}_volume",
                f"{symbol}_close_time",
                f"{symbol}_quote_value",
                f"{symbol}_n_trades",
                f"{symbol}_taker_buy_base",
                f"{symbol}_taker_buy_quote",
                "_",
            ],
            dtype=float,
        )
        _["date"] = _["date"].apply(
            lambda x: datetime.fromtimestamp(int(x) / 1000, UTC)
        )
        _[f"{symbol}_close_time"] = _[f"{symbol}_close_time"].apply(
            lambda _: datetime.fromtimestamp(_ / 1000, UTC)
        )
        _ = _.set_index("date").sort_index()
        _ = _[~_.index.duplicated(keep="first")]

        return _[
            [
                f"{symbol}_open",
                f"{symbol}_high",
                f"{symbol}_low",
                f"{symbol}_close",
            ]
        ]

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
        for a in tqdm(asset, desc="Getting Price History"):
            _ = []
            for start, end in break_date_range_into_intervals(
                start_time=start_time,
                end_time=end_time,
                interval_size=self.SIZE_LIMIT,
                interval_unit="hours",
            ):
                _.append(
                    self._get_history(
                        symbol=a,
                        interval="1h",
                        start_time=start,
                        end_time=end,
                    )
                )

            _ = pd.concat(_)
            _df.append(_[~_.index.duplicated(keep="first")].reindex(timeline))
        _df = pd.concat(_df, axis=1)
        _df.index.name = "Date"

        if save:
            print("Saving downloaded data...")
            self.save_file(_df)

        return _df
