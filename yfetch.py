import asyncio
import click
import utils
import yfinance as yf
from sqlalchemy.dialects.sqlite import insert
import os
from logger import logger
import pandas as pd
import numpy as np
import datetime as dt

DB_DIR = './parquet'

async def upsert(tickers, eod_data):
    # ensure we only have weekdays
    eod_data = eod_data.loc[eod_data.index.weekday < 5] 
    for ric in tickers:
        logger.info(f'Processing {ric}')
        if ric in eod_data.columns.get_level_values(0):
            data = eod_data[ric]
            # derive the adjustment factor
            for year, data in eod_data[ric].groupby(eod_data.index.year):
                # excluded nans
                data = data.loc[np.isfinite(data['close_px'])]
                # note: there is no point in saving adj_close_px as its backward
                # adjusted so purely depends on the start and end date window 
                # requested by the client, we can potentially save c2c_ret but
                # its a straight forward calculation which can be done on the fly
                # in the service api, so we just calculate the adj_factor applicable
                # on the day t and save it along with ohlc and volume
                adjs = data['adj_factor'].ffill()/data['close_px'].ffill()
                data['adj_factor'] = (adjs[::-1]/adjs[::-1].shift(1).fillna(1.0))[::-1]
                # a tab on how much data is being updated
                data_len = len(data)
                dir_path = os.path.join(DB_DIR, f'{ric}')
                os.makedirs(dir_path, exist_ok=True)
                file_path = os.path.join(dir_path, f'{year}.parquet')
                # need to check if file already exists, if it does then we need to
                # update the existing version...
                if os.path.exists(file_path):
                    existing = pd.read_parquet(file_path)
                    # ignore those dates which are there in data
                    ignored = np.isin(existing.index, data.index)
                    data = pd.concat((existing[~ignored], data), sort=True)
                # hold a pointer to data for checks
                data_ = data
                # if we dont have much as we could be at the start of the
                # year, we have to load a bit for checks
                if len(data) < 20:
                    lookback_date = data.index[0].date()-dt.timedelta(2*20)
                    if lookback_date.year < year:
                        file_path = os.path.join(dir_path, f'{lookback_date.year}.parquet')
                        if os.path.exists(file_path):
                            sel = [("date",">=",lookback_date)]
                            lookback_data = pd.read_parquet(file_path, filters=sel)
                            data_ = pd.concat((lookback_data, data_), sort=True)
                # data checks, winsorisation if any..
                if len(data_) > 20:
                    # detect gaps
                    max_gap = data_.index.diff().days.fillna(0).max()
                    if max_gap >= 5:
                        logger.warning(f'{max_gap} days gap detected for {ric}, please check!!')
                    # check if there is an outlier in the data
                    # this can be based on either percentage change
                    # or some std away from a rolling average
                    roll_adj_cum_prod = data_['adj_factor'][::-1].cumprod()[::-1]
                    adj_close_px = data_['close_px'].multiply(roll_adj_cum_prod, axis='rows')
                    adj_close_px.name = 'adj_close_px'
                    c2c_ret = ((adj_close_px.ffill() / adj_close_px.ffill().shift(1)) - 1.0)*100.0
                    c2c_ret.name = 'c2c_ret'
                    # rolling mean of 5 days
                    mu = adj_close_px.shift(1).rolling(5).mean()
                    sd = adj_close_px.shift(1).rolling(5).std()
                    min_level = mu - 5 * sd # < 5 std
                    max_level = mu + 5 * sd # > 5 std
                    check_max = adj_close_px > max_level 
                    check_min = adj_close_px < min_level 
                    # to show the previous entry to the outlier
                    check_max = check_max | check_max.shift(-1)
                    check_min = check_min | check_min.shift(-1)
                    #check_max = check_max.fillna(False).infer_objects(copy=False)
                    #check_min = check_min.fillna(False).infer_objects(copy=False)
                    # only check for the length of the new data
                    check_max.iloc[:-data_len] = False
                    check_min.iloc[:-data_len] = False
                    if np.any(check_max):
                        logger.warning(f'Detected outliers for {ric} > +5 std (based on 5 day rolling mean), please check!')
                        logger.warning(f"\n{pd.concat((data_['close_px'],adj_close_px,c2c_ret),axis=1)[check_max]}")
                    if np.any(check_min):
                        logger.warning(f'Detected outliers for {ric} < -5 std (based on 5 day rolling mean), please check!')
                        #logger.warning(f"\n{data_.loc[check_min,['close_px','c2c_ret']]}")
                        logger.warning(f"\n{pd.concat((data_['close_px'],adj_close_px,c2c_ret),axis=1)[check_min]}")

                # write to parquet file
                data.to_parquet(file_path, index=True, compression='gzip')

async def download(tickers, period, batch=5):
    tickers = tickers.split(',')
    batches = [tickers[i:i+batch] for i in range(0, len(tickers), batch)]
    for batch in batches:
        logger.info(f'Processing batch: {batch}')
        eod_data = yf.download(tickers=' '.join(batch), period=period, group_by="tickers",auto_adjust=False)
        if not eod_data.empty:
            eod_data.rename(columns={'Open':'open_px','High':'high_px','Low':'low_px','Close':'close_px',
                                     'Adj Close':'adj_factor','Volume':'volume'}, inplace=True)
            eod_data.index.name = 'date'
            eod_data.columns.names=['ticker','price']
            await upsert(batch, eod_data)

@click.command()
@click.option('--tickers',
              type=click.STRING,
              default='NVDA,MSFT,AAPL,GOOG,AMZN,META,AVGO,TSLA,JPM,WMT,ORCL,LLY,V,MA,NFLX,XOM,JNJ,ABBV,PLTR,COST,HD,BAC,PG',
              required=False,
              show_default=True,
              help='ric or , seperated list of rics')
@click.option('--period',
              type=click.STRING,
              default='5d',
              required=False,
              show_default=True,
              help='1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max')
def main(tickers, period):
    """yfinance downloader"""
    asyncio.run(download(tickers, period))

if __name__ == "__main__":
    main()
