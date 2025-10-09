import click
from gunicorn.app.base import BaseApplication
from fastapi import FastAPI, Depends, HTTPException, Response
from fastapi.middleware.gzip import GZipMiddleware
from datetime import date
import glob
from pathlib import Path
from utils import *
import datetime as dt
import os
import pandas as pd
import numpy as np
from async_lru import alru_cache

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

DB_DIR = os.path.join(os.path.dirname(__file__), "parquet")

@alru_cache(maxsize=1)
async def _get_tickers():
    path = Path(DB_DIR)
    tickers = pd.Series(data=np.sort([p.name for p in path.glob("*") if p.is_dir()]))
    return tickers

@app.get("/tickers/")
async def get_tickers():
    tickers = await _get_tickers()
    if tickers.empty:
        raise HTTPException(status_code=404, detail="No Tickers found")
    return Response(tickers.to_json(orient='records'), media_type='application/json')

@alru_cache(maxsize=64)
async def _get_cached_returns_by_ticker(ticker):
    path = Path(os.path.join(DB_DIR, f'{ticker}'))

    file_paths = np.sort([p for p in path.glob("*")])

    eod_data = pd.concat([pd.read_parquet(file_path, columns=['close_px','adj_factor'])
                         for file_path in file_paths], sort=False, copy=False)

    if eod_data.empty:
        raise HTTPException(status_code=404, detail="No Ticker/Dates found")

    # if the client requires backward adjusted close
    #eod_data['roll_adj_cum_prod'] = eod_data['adj_factor'][::-1].cumprod()[::-1]
    #eod_data['adj_close_px'] = eod_data['close_px'].multiply(eod_data['roll_adj_cum_prod'], axis='rows')
    #eod_data['c2c_ret'] = (eod_data['adj_close_px'].ffill() / eod_data['adj_close_px'].ffill().shift(1)) - 1.0

    # incase only return is required
    eod_data['c2c_ret'] = (eod_data['close_px'].ffill() / (eod_data['close_px'].multiply(eod_data['adj_factor'], axis='rows')).ffill().shift(1)) - 1.0
    eod_data.fillna(value={'c2c_ret':0.0},inplace=True)

    return eod_data

async def _get_returns_by_ticker(ticker, start_date, end_date, include_ric=False):
    db_tickers = await _get_tickers()
    if db_tickers.empty:
        raise HTTPException(status_code=404, detail="No Tickers found")

    if not ticker in db_tickers.values:
        raise HTTPException(status_code=404, detail=f"Ticker: {ticker} not found")

    eod_data = await _get_cached_returns_by_ticker(ticker)

    if eod_data.empty:
        raise HTTPException(status_code=404, detail="No Ticker/Dates found")

    #eod_data = eod_data.loc[(eod_data.index.date >= start_date.date()) & (eod_data.index.date <= end_date.date()),:]
    eod_data = eod_data.loc[start_date:end_date]

    if eod_data.empty:
        raise HTTPException(status_code=404, detail="No Ticker/Dates found")

    return eod_data[['c2c_ret']].rename(columns={'c2c_ret':ticker}) if include_ric else eod_data[['c2c_ret']]

async def _get_returns_by_tickers(tickers, start_date, end_date):
    tickers = np.array([ticker.upper() for ticker in tickers.split(',')])

    db_tickers = await _get_tickers()
    if db_tickers.empty:
        raise HTTPException(status_code=404, detail="No Tickers found")

    missing = ~np.isin(tickers, db_tickers)
    if np.any(missing):
        raise HTTPException(status_code=404, detail=f"Tickers: {tickers[missing]} not found")
    
    # get years between start and end date
    start_date = parse_date(start_date)
    if not start_date:
        raise HTTPException(status_code=404, detail="Invalid Start Date")
    end_date = parse_date(end_date)
    if not end_date:
        raise HTTPException(status_code=404, detail="Invalid End Date")
    if end_date < start_date:
        raise HTTPException(status_code=404, detail="End Date < Start Date")

    eod_data = pd.concat([await _get_returns_by_ticker(ticker, start_date, end_date, include_ric=len(tickers)>1)
                         for ticker in tickers], sort=False, copy=False, axis=1)

    if eod_data.empty:
        raise HTTPException(status_code=404, detail="No Ticker/Dates found")

    return eod_data

@app.get("/returns/{tickers}/{start_date}/{end_date}")
async def get_returns_by_tickers(tickers, start_date, end_date):
    eod_data = await _get_returns_by_tickers(tickers, start_date, end_date)

    return Response(eod_data.reset_index().to_json(orient='records',date_format='iso'), media_type='application/json')

@app.get("/returns/{query_date}/{tickers}")
async def get_returns_by_date(query_date, tickers):
    # get year 
    query_date = parse_date(query_date)
    if not query_date:
        raise HTTPException(status_code=404, detail="Invalid Query Date")

    eod_data = await _get_returns_by_tickers(tickers, str(query_date), str(query_date))

    return Response(eod_data.reset_index().to_json(orient='records',date_format='iso'), media_type='application/json')

@click.command()
@click.option('--host',
              type=click.STRING,
              default='127.0.0.1',
              required=False,
              show_default=True,
              help='host')
@click.option('--port',
              type=click.INT,
              default=8000,
              required=False,
              show_default=True,
              help='port')
def main(host, port):
    """yfinance rest service"""
    #import uvicorn
    #uvicorn.run(app, host=host, port=port)
    
    from gunicorn.app.base import BaseApplication
    from uvicorn.workers import UvicornWorker
    class Gunicorn(BaseApplication):
        def __init__(self, app, options=None):
            self.app = app
            self.options = options
            super().__init__()

        def load_config(self):
            for key, value in self.options.items():
                if key in self.cfg.settings and value is not None:
                    self.cfg.set(key.lower(), value)

        def load(self):
            return self.app

    options = {'bind': f'{host}:{port}',
               'workers': 8,
               'worker_class': UvicornWorker}

    Gunicorn(app, options).run()


if __name__ == "__main__":
    main()
