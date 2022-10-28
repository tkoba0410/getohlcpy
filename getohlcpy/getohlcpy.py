from datetime import timedelta, timezone
import os
from typing import List
import requests
import pandas as pd
from requests.models import Response

TZ_JTC = timezone(timedelta(hours=+9))


def _get_ohlc_response(
        pair: str) -> Response:
    # exchange (str): 'bitflyer'
    # pair (str): 'btcfxjpy', 'btcjpy'
    url = f"https://api.cryptowat.ch/markets/bitflyer/{pair}/ohlc"
    params = {'periods': 60, 'after': 978274800}  # DATE_2001_01_01
    return requests.get(url, params)


def _response_to_df(res: Response) -> pd.DataFrame:
    json = res.json(
        parse_float=float,
        parse_int=float,
    )['result']
    period = list(json.keys())[0]
    df = pd.DataFrame(
        data=json[period],
        columns=[
            'CloseTime',
            'Open',
            'High',
            'Low',
            'Close',
            'Volume',
            'QuoteVolume'
        ]
    )
    return df


def _reformat(df: pd.DataFrame) -> pd.DataFrame:
    df['OpenTime'] = df['CloseTime'] - int(60)
    df['OpenTime'] = pd.to_datetime(df['OpenTime'], unit='s', utc=True)
    df['OpenTime'] = df['OpenTime'].dt.tz_convert(TZ_JTC)
    df = df.set_index('OpenTime')
    df = df[[
        'Open',
        'High',
        'Low',
        'Close',
        'Volume',
        'QuoteVolume'
    ]]
    return df


def _fillna_ohlcv(
    df: pd.DataFrame,
) -> pd.DataFrame:
    date = pd.date_range(start=df.index[0], end=df.index[-1], freq='1min')
    ts = pd.DataFrame(range(len(date)), index=date, columns=['ts'])
    df = df.merge(ts, how='outer', right_index=True, left_index=True)
    df = df.drop(columns=['ts'])
    df['Close'] = df['Close'].interpolate('ffill')
    df.iloc[:, :4] = df.iloc[:, :4].interpolate(axis=1, method='bfill')
    df['Volume'] = df['Volume'].fillna(0)
    df['QuoteVolume'] = df['QuoteVolume'].fillna(0)
    return df


def _csv_merge(
        df: pd.DataFrame,
        archive_file: str) -> pd.DataFrame:
    df_cashe = load_ohlcv(archive_file)
    df_diff = df[~(df.index).isin(df_cashe.index[:-1])]
    df_result = pd.concat([df_cashe, df_diff], axis=0)
    df_result.index.name = 'OpenTime'
    return df_result


def get_ohlcv(pair: str) -> pd.DataFrame:
    res = _get_ohlc_response(pair)
    df = _response_to_df(res)
    df = _reformat(df)
    df = _fillna_ohlcv(df)
    return df


def get_ohlcv_with_archive(
        pair: str,
        archive_file: str = None,
        archive_update: bool = False) -> pd.DataFrame:
    df = get_ohlcv(pair)
    if isinstance(archive_file, str):
        if os.path.exists(archive_file):
            df = _csv_merge(df, archive_file)
            if archive_update:
                df.to_csv(archive_file)
    return df


def load_ohlcv(archive_file: str) -> pd.DataFrame:
    return pd.read_csv(archive_file, header=0, index_col=0, parse_dates=True)


def load_ohlcv_with_cashe(
        pair: str,
        pkl_cashe_path: str,
        archive_file: str = None,
        archive_update: bool = False,
) -> pd.DataFrame:
    if os.path.isfile(pkl_cashe_path):
        df = pd.read_pickle(pkl_cashe_path)
        return df
    else:
        df: pd.DataFrame = get_ohlcv_with_archive(
            pair=pair,
            archive_file=archive_file,
            archive_update=archive_update
        )
        df.to_pickle(pkl_cashe_path)
        return df


def info() -> List[str]:
    pairs = ['btcfxjpy', 'btcjpy']
    print(pairs)
    return pairs


if __name__ == '__main__':
    df = load_ohlcv_with_cashe(
        pair='btcfxjpy',
        pkl_cashe_path='csv/ohlcv-btcfxjpy.pkl',
        archive_file='csv/ohlcv-btcfxjpy.csv',
        archive_update=True)
    df = load_ohlcv_with_cashe(
        pair='btcjpy',
        pkl_cashe_path='csv/ohlcv-btcjpy.pkl',
        archive_file='csv/ohlcv-btcjpy.csv',
        archive_update=True)


"""
https://docs.cryptowat.ch/rest-api/markets/ohlc

OHLC Candlesticks
This is the data that is shown in our
.
https://api.cryptowat.ch/markets/:exchange/:pair/ohlc

Market OHLC

Returns a market's OHLC candlestick data. A series of candlesticks is
represented as a list of lists of numbers.

Parameters

Path

exchange    string  Exchange symbol
pair        string  Pair symbol

Query

before  integer Unix timestamp. Only return candles opening before this time. Example: 1481663244
after   integer Unix timestamp. Only return candles opening after this time. Example 1481663244
periods array   Comma separated integers. Only return these time periods. Example: 60,180,108000

Responses

200

Querying OHLC data within a specific time-range, it is better to define both
the after and before parameters for more accurate results.

When both after and before parameters are defined, the command will
 query and fetch OHLC data for the defined range.

When only after has been specified, the before parameter will be
 updated to the current date (now) and the results will be within the
  range: after until now.

Supplying before without an after relies on cached data and returns
variable results.


Response Format

1-minute candles are under the "60" key. 3-minute are "180", and so on.
The values are in this order:

[
  CloseTime,
  OpenPrice,
  HighPrice,
  LowPrice,
  ClosePrice,
  Volume,
  QuoteVolume
]

So for instance, we can take this string value under Coinbase Pro's BTCUSD
 market for time period "3600" (1-hour):
[
    1474736400,
    8744,
    8756.1,
    8710,
    8753.5,
    91.58314308,
    799449.488966417
],

This represents a 1-hour candle starting at 1474732800 (Sat, 24 Sep
2016 16:00:00 UTC) and ending at 1474736400 (Sat, 24 Sep 2016
17:00:00 UTC).
The open price for this candle is 8744, the high price 8756.1, the low price
8710, and the close price 8753.5. The volume for this candle was
91.58314308 denominated in BTC, and 799449.488966417 denominated in USD.


Period values

Value   Label

60      1m
180     3m
300     5m
900     15m
1800    30m
3600    1h
7200    2h
14400   4h
21600   6h
43200   12h
86400   1d
259200  3d
604800  1w
604800_Monday   1w (weekly start Monday)

"""
