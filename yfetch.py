import asyncio
import click
from utils import *
import yfinance as yf
from sqlalchemy.dialects.sqlite import insert
import os
from logger import logger
import pandas as pd
import numpy as np
import datetime as dt
from yserv import _get_tickers
import sys

app_config = ApplicationConfig(__file__)
base_dir = app_config.data.base_dir()

#DB_DIR = os.path.join(os.path.dirname(__file__), "parquet")
DB_DIR = app_config.data.db_dir()

async def upsert(tickers, eod_data):
    # ensure we only have weekdays
    eod_data = eod_data.loc[eod_data.index.weekday < 5] 
    for ric in tickers:
        logger.info(f'Processing {ric}')
        if ric in eod_data.columns.get_level_values('ticker'):
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
                    # get the last date of existing data
                    existing_end_date = existing.index[-1].date()
                    data_start_date = data.index[0].date()
                    delta_days = data_start_date - existing_end_date
                    if delta_days.days >= 5:
                        logger.error(f'New data insertion for {ric} creates a gap of >= 5 days [{existing_end_date} : {data_start_date}], please check!! aborting...')
                        sys.exit(1)
                    # ignore those dates which are there in data
                    ignored = np.isin(existing.index, data.index)
                    data = pd.concat([existing[~ignored], data], sort=False).sort_index()
                    
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
                            data_ = pd.concat((lookback_data, data_), sort=False).sort_index()
                # data checks, winsorisation if any..
                if len(data_) > 20:
                    # detect gaps
                    gap_ts = data_.index.diff().days.fillna(0)
                    max_gap = gap_ts.max()
                    if max_gap >= 5:
                        max_gap_idx = np.argmax(gap_ts)
                        logger.error(f'{max_gap} days gap detected for {ric}, please check!!')
                        logger.error(f'\n{data_.iloc[max_gap_idx-1:max_gap_idx+1]}')
                    # check if there is an outlier in the data
                    # this can be based on either percentage change
                    # or some std away from a rolling average
                    roll_adj_cum_prod = data_['adj_factor'][::-1].cumprod()[::-1]
                    adj_close_px = data_['close_px'].multiply(roll_adj_cum_prod, axis='rows')
                    adj_close_px.name = 'adj_close_px'
                    c2c_ret = ((adj_close_px.ffill() / adj_close_px.ffill().shift(1)) - 1.0)*100.0
                    c2c_ret.name = 'c2c_ret'
                    # rolling mean of 5 days
                    mu = c2c_ret.shift(1).rolling(5).mean()
                    sd = c2c_ret.shift(1).rolling(5).std()
                    min_level = mu - 5 * sd # < 5 std
                    max_level = mu + 5 * sd # > 5 std
                    check_max = c2c_ret > max_level 
                    check_min = c2c_ret < min_level 
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
                    check_stale = c2c_ret == 0.0
                    check_stale = check_stale | check_stale.shift(-1)
                    check_stale.iloc[:-data_len] = False
                    if np.any(check_stale):
                        logger.warning(f'Detected stale prices for {ric}, please check!')
                        logger.warning(f"\n{pd.concat((data_['close_px'],adj_close_px,c2c_ret),axis=1)[check_stale]}")

                # write to parquet file
                data.to_parquet(file_path, index=True, compression='gzip')

async def download(tickers, period, start_date, end_date, batch=5):
    tickers = np.array(tickers.split(','))
    batches = [(tickers[i:i+batch], (period,start_date,end_date)) for i in range(0, len(tickers), batch)]
    if period:
        # need to detect last entry for each ric and batch accordingly!! more work...
        if period == 'auto':
            end_date = yesterday(business_day=True)
            batches = []
            _get_tickers.cache_clear()
            db_tickers = (await _get_tickers()).set_index('ticker')
            # what if there are new tickers !!??
            # we need a default start_date else complain and exit
            new_tickers_mask = ~np.isin(tickers, db_tickers.index.values)
            if np.any(new_tickers_mask):
                if not start_date:
                    logger.warning(f"New tickers provided {tickers[new_tickers_mask]},\ncant identify default start date for new tickers with '--period=auto' mode, please also provide '--start_date'!! aborting...")
                    sys.exit(1)
                new_tickers = tickers[new_tickers_mask]
                batches = [(new_tickers[i:i+batch], (None,start_date,end_date)) for i in range(0, len(new_tickers), batch)]
                
            # get the subset of the tickers we are interested in
            tickers_subset = np.isin(db_tickers.index.values,tickers)
            db_tickers = db_tickers.loc[tickers_subset]
            db_tickers = db_tickers.reset_index()[['ticker','end_date']].groupby('end_date')['ticker'].apply(list)
            #db_tickers.reset_index()[['ticker','end_date']].groupby('end_date',as_index=False)['ticker'].apply(list)
            for item in db_tickers.items():
                start_date = item[0].to_pydatetime()
                upsert_tickers = [str(t) for t in item[1]]
                batches.append([(upsert_tickers[i:i+batch], (None,start_date,end_date)) for i in range(0, len(upsert_tickers), batch)][0])

    for batch in batches:
        batch_tickers = batch[0]
        batch_period, batch_start_date, batch_end_date = batch[1]
        if batch_period:
            logger.info(f'Processing batch: tickers: {batch_tickers}, period: {batch_period}')
            eod_data = yf.download(tickers=' '.join(batch_tickers), period=batch_period, group_by="tickers",auto_adjust=False)
        else:
            logger.info(f'Processing batch: tickers: {batch_tickers}, start_date: {batch_start_date.date()}, end_date: {batch_end_date.date()}')
            # to make end_date inclusive we add a day to it
            eod_data = yf.download(tickers=' '.join(batch_tickers), start=batch_start_date, end=batch_end_date+dt.timedelta(days=1), group_by="tickers",auto_adjust=False)
        if not eod_data.empty:
            eod_data.rename(columns={'Open':'open_px','High':'high_px','Low':'low_px','Close':'close_px',
                                     'Adj Close':'adj_factor','Volume':'volume'}, inplace=True)
            eod_data.index.name = 'date'
            eod_data.columns.names=['ticker','price']
            await upsert(batch_tickers, eod_data)

@click.command()
@click.option('--tickers',
              type=click.STRING,
              default='NVDA,MSFT,AAPL,GOOG,AMZN,META,AVGO,TSLA,JPM,WMT,ORCL,LLY,V,MA,NFLX,XOM,JNJ,ABBV,PLTR,COST,HD,BAC,PG',
              required=False,
              show_default=True,
              help='ric or , seperated list of rics')
@click.option('--period',
              type=click.STRING,
              default=None,
              required=False,
              show_default=True,
              help='1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max,auto\n[auto: automatically detect last available date for present tickers and backfills till yesterday, please also provide --start_date if there are any new tickers and auto option is used, start_date will be applied on new tickers only]')
@click.option('--start_date',
              type=click.DateTime(formats=valid_date_formats),
              default=None,
              required=False,
              show_default=True,
              help='query start date')
@click.option('--end_date',
              type=click.DateTime(formats=valid_date_formats),
              default=None,
              required=False,
              show_default=True,
              help='query end date')
def main(tickers, period, start_date, end_date):
    """yfinance downloader"""
    logger.info(f"DB_DIR: {DB_DIR}")

    if period:
        if period == 'auto':
            if end_date:
                raise click.BadParameter(f"'--period={period}' cant be provided with '--end_date'")
        else:
            if start_date or end_date:
                raise click.BadParameter(f"'--period={period}' cant be provided with '--start_date' and/or '--end_date'")

    if start_date or end_date:
        if not start_date:
           raise click.BadParameter("'--end_date' cant be provided without '--start_date'")
        if not end_date and not period == 'auto':
           raise click.BadParameter("'--start_date' cant be provided without '--end_date'")
        if start_date and end_date and start_date > end_date:
           raise click.BadParameter("'--start_date' cant be > than '--end_date'")

    if not period and not start_date and not end_date:
        raise click.BadArgumentUsage("Please provide an option '--period' or a combination of '--start_date' and '--end_date'") 

    asyncio.run(download(tickers, period, start_date, end_date))

if __name__ == "__main__":
    main()
