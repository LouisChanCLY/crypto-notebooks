from datetime import UTC, datetime, timedelta
import json
from typing import List
import pandas as pd
from tqdm import tqdm
import requests

BINANCE_DOWNLOAD_FILE_NAME = "binance_download.csv"


def get_interest_history(
    asset: str, vipLevel: int, startTime: datetime, endTime: datetime, size: int = 90
) -> pd.DataFrame:
    """Get historical interest rates"""
    response = json.loads(
        requests.get(
            "https://www.binance.com/bapi/margin/v1/public/margin/vip/spec/history-interest-rate",
            params={
                "asset": asset,
                "vipLevel": vipLevel,
                "size": size,
                "startTime": int(
                    startTime.timestamp() * 1000
                ),  # Convert to timestamp in ms
                "endTime": int(
                    endTime.timestamp() * 1000
                ),  # Convert to timestamp in ms
            },
        ).content
    )
    _ = pd.DataFrame(response["data"])
    _["dailyInterestRate"] = pd.to_numeric(_["dailyInterestRate"])
    _["datetime"] = _["timestamp"].apply(
        lambda x: datetime.fromtimestamp(int(x) / 1000, UTC)
    )
    return _.set_index("datetime").sort_index().drop_duplicates()


def get_price_history(
    client, symbol: str, interval: str, startTime: datetime, endTime: datetime
) -> pd.DataFrame:
    _ = pd.DataFrame(
        client.klines(
            symbol=symbol,
            interval=interval,
            startTime=int(startTime.timestamp() * 1000),  # Convert to timestamp in ms
            endTime=int(endTime.timestamp() * 1000),  # Convert to timestamp in ms
            limit=1000,
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
    _["date"] = _["date"].apply(lambda x: datetime.fromtimestamp(int(x) / 1000, UTC))
    _[f"{symbol}_close_time"] = _[f"{symbol}_close_time"].apply(
        lambda _: datetime.fromtimestamp(_ / 1000, UTC)
    )
    _ = _.set_index("date").sort_index()
    _ = _.drop_duplicates()

    return _[
        [
            f"{symbol}_open",
            f"{symbol}_high",
            f"{symbol}_low",
            f"{symbol}_close",
        ]
    ]


def get_month_list(start_date: datetime, end_date: datetime) -> List[datetime]:
    dates = []
    _ = start_date
    while _ < end_date:
        dates.append(_)
        _ += pd.DateOffset(months=1)
    dates.append(end_date)
    return dates


def load_data(
    api_key,
    api_secret,
    interest_rate_assets: List[str],
    price_assets: List[str],
    start_date: datetime,
    end_date: datetime,
):
    from binance.spot import Spot

    months = get_month_list(start_date, end_date)
    timeline = pd.date_range(start=start_date, end=end_date, freq="H", tz="UTC")
    client = Spot(api_key, api_secret)

    df_rates = []
    for asset in tqdm(interest_rate_assets, desc="Getting Interest Rates"):
        _df = []
        for start, next_start in zip(months[:-1], months[1:]):
            _df.append(
                get_interest_history(
                    asset=asset,
                    vipLevel=0,
                    startTime=start,
                    endTime=next_start + timedelta(hours=-1),
                )
            )
        _df = pd.concat(_df)
        df_rates.append(
            _df.reindex(timeline)["dailyInterestRate"].ffill().rename(asset)
        )
    df_rates = pd.concat(df_rates, axis=1)

    df_price = []
    for asset in tqdm(price_assets, desc="Getting Prices"):
        _df = []
        for start, next_start in zip(months[:-1], months[1:]):
            _df.append(
                get_price_history(
                    client=client,
                    symbol=asset,
                    interval="1h",
                    startTime=start,
                    endTime=next_start + timedelta(hours=-1),
                )
            )
        df_price.append(pd.concat(_df).reindex(timeline))
    df_price = pd.concat(df_price, axis=1)
    df = df_rates.join(df_price)
    df.index.name = "Date"
    print("Saving downloaded data...")
    df.to_csv(BINANCE_DOWNLOAD_FILE_NAME)
    return df
