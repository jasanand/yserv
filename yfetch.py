import asyncio
import click
import utils
import yfinance as yf
from sqlalchemy.dialects.sqlite import insert
import os
from logger import logger
import pandas as pd
import numpy as np

DB_DIR = './parquet'

async def upsert(tickers, eod_data):
    for ric in tickers:
        logger.info(f'Processing {ric}')
        if ric in eod_data.columns.get_level_values(0):
            data = eod_data[ric]
            # derive the adjustment factor
            adjs = eod_data[(ric, 'adj_factor')].ffill()/eod_data[(ric, 'close_px')].ffill()
            eod_data[(ric, 'adj_factor')] = (adjs[::-1]/adjs[::-1].shift(1).fillna(1.0))[::-1]
            # data checks, winsorisation if any..
            for year, data in eod_data[ric].groupby(eod_data.index.year):
                dir_path = os.path.join(DB_DIR, f'{ric}')
                os.makedirs(dir_path, exist_ok=True)
                file_path = os.path.join(dir_path, f'{year}.parquet')
                # need to check if file already exists, if it does then we need to
                # update the existing version...
                if os.path.exists(file_path):
                    existing = pd.read_parquet(file_path)
                    # ignore those dates which are there in data
                    ignored = np.isin(existing.index, data.index)
                    existing = pd.concat((existing[~ignored], data), sort=True)
                    existing.to_parquet(file_path, index=True, compression='gzip')
                else:
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
