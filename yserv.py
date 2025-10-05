import click
from fastapi import FastAPI, Depends, HTTPException, Response
from fastapi.middleware.gzip import GZipMiddleware
from datetime import date
import glob
from pathlib import Path
from utils import *
import datetime as dt
import os
import pandas as pd

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

DB_DIR = os.path.join(os.path.dirname(__file__), "parquet")

@app.get("/tickers/")
async def get_tickers():
    path = Path(DB_DIR)
    tickers = pd.Series(data=np.sort([p.name for p in path.glob("*") if p.is_dir()]))
    if tickers.empty:
        raise HTTPException(status_code=404, detail="No Tickers found")
    return Response(tickers.to_json(orient='records'), media_type='application/json')

async def get_returns_by_ticker(ticker, start_date, end_date, include_ric=False):
    years = np.arange(start_date.year, end_date.year+1, 1)
    if len(years) == 0:
        years = [start_date.year]

    file_paths = []
    for year in years:
        file_path = os.path.join(DB_DIR, f'{ticker}', f'{year}.parquet')
        if os.path.exists(file_path):
            file_paths.append(file_path)

    sel = [("date",">=",start_date), ("date","<=",end_date)]
    eod_data = pd.concat([pd.read_parquet(file_path, columns=['close_px','adj_factor'], filters=sel)
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
    if include_ric:
        eod_data['ticker'] = ticker
        return eod_data[['ticker','c2c_ret']]
    else:
        return eod_data[['c2c_ret']]

@app.get("/returns/{ticker}/{start_date}/{end_date}")
async def get_returns(ticker, start_date, end_date):
    ticker = ticker.upper()

    path = Path(DB_DIR)
    tickers = pd.Series(data=np.sort([p.name for p in path.glob("*") if p.is_dir()]))

    if tickers.empty or not ticker in tickers.values:
        raise HTTPException(status_code=404, detail="No Tickers found")
    
    # get years between start and end date
    start_date = parse_date(start_date)
    if not start_date:
        raise HTTPException(status_code=404, detail="Invalid Start Date")
    end_date = parse_date(end_date)
    if not end_date:
        raise HTTPException(status_code=404, detail="Invalid End Date")
    if end_date < start_date:
        raise HTTPException(status_code=404, detail="End Date > Start Date")

    eod_data = await get_returns_by_ticker(ticker, start_date, end_date)
    #return Response(eod_data[['c2c_ret']].to_json(orient='table',date_format='iso'), media_type='application/json')
    return Response(eod_data[['c2c_ret']].reset_index().to_json(orient='records',date_format='iso'), media_type='application/json')

@app.get("/returns/{query_date}/{tickers}")
async def get_returns_by_date(query_date, tickers):
    tickers = [ticker.upper() for ticker in tickers.split(',')]

    path = Path(DB_DIR)
    db_tickers = pd.Series(data=np.sort([p.name for p in path.glob("*") if p.is_dir()]))
    if db_tickers.empty:
        raise HTTPException(status_code=404, detail="No Tickers found")

    for ticker in tickers:
        if not ticker in db_tickers.values:
            raise HTTPException(status_code=404, detail=f"Ticker: {ticker} not found")
    
    # get year 
    query_date = parse_date(query_date)
    if not query_date:
        raise HTTPException(status_code=404, detail="Invalid Query Date")

    # need some data for returns as we calculate them on the fly
    start_date = query_date-dt.timedelta(days=5)
    end_date = query_date

    eod_data = pd.concat([await get_returns_by_ticker(ticker, start_date, end_date, include_ric=True)
                         for ticker in tickers], sort=False, copy=False)

    if eod_data.empty:
        raise HTTPException(status_code=404, detail="No Ticker/Dates found")

    eod_data = pd.pivot_table(eod_data, index=eod_data.index, columns=['ticker'], values=['c2c_ret'])
    eod_data = eod_data.droplevel(0, axis=1)
    eod_data.fillna(0.0,inplace=True)
    eod_data = eod_data.loc[eod_data.index.date==query_date.date(),:]

    #return Response(eod_data.to_json(orient='table',date_format='iso'), media_type='application/json')
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
    import uvicorn
    uvicorn.run(app, host=host, port=port)

if __name__ == "__main__":
    main()
