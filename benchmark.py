import google_benchmark as benchmark
import requests
from multiprocessing import Pool
import datetime as dt
import pandas as pd
import numpy as np

YSERV_URL = 'http://127.0.0.1:8000'

date_range = pd.bdate_range(start='2023-10-04',end='2025-10-03')

np.random.seed(0)

tickers = 'NVDA,MSFT,AAPL,GOOG,AMZN,META,AVGO,TSLA,JPM,WMT,ORCL,LLY,V,MA,NFLX,XOM,JNJ,ABBV,PLTR,COST,HD,BAC,PG'
tickers_list = tickers.split(',')

def get_date():
    return date_range[np.random.randint(0,len(date_range)-5)]

def get_start_end_date():
    start_date = get_date()
    delta_days = (date_range[-1] - start_date).days
    end_date = start_date + dt.timedelta(days=max(5,np.random.randint(0,delta_days)))
    return (start_date, end_date)

def get_ticker():
    return tickers_list[np.random.randint(0,len(tickers_list))]

def get_tickers():
    rtickers = []
    rlen = np.random.randint(1,len(tickers_list))
    for i in range(1,max(rlen,2)):
        rtickers.append(get_ticker())
    rtickers = np.unique(rtickers)
    return ','.join(rtickers)

@benchmark.register(name='[url: /tickers] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_tickers(state):
    while state:
        response = requests.get(f'{YSERV_URL}/tickers')

def _get_returns_by_ticker_url(randomize=False):
    if not randomize:
        return f'{YSERV_URL}/returns/AAPL/20230101/20251003'
    else:
        start_date,end_date = get_start_end_date()
        ticker = get_ticker()
        url = f'{YSERV_URL}/returns/{ticker}/{str(start_date.date())}/{str(end_date.date())}'
        #print(url)
        return url

@benchmark.register(name='[url: /returns/ticker/start_date/end_date] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_ticker(state):
    while state:
        response = requests.get(_get_returns_by_ticker_url())

@benchmark.register(name='[url: /returns/ticker/start_date/end_date][random params] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_ticker_randomized(state):
    while state:
        response = requests.get(_get_returns_by_ticker_url(randomize=True))

def _get_returns_by_tickers_url(randomize=False):
    if not randomize:
        return f'{YSERV_URL}/returns/{tickers}/20230101/20251003'
    else:
        start_date,end_date = get_start_end_date()
        tickers_ = get_tickers()
        url = f'{YSERV_URL}/returns/{tickers_}/{str(start_date.date())}/{str(end_date.date())}'
        #print(url)
        return url

def _get_returns_by_tickers(randomize=False):
    response = requests.get(_get_returns_by_tickers_url(randomize))

@benchmark.register(name='[url: /returns/tickers/start_date/end_date] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_tickers(state):
    while state:
        _get_returns_by_tickers()

@benchmark.register(name='[url: /returns/tickers/start_date/end_date][random args] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_tickers_randomize(state):
    while state:
        _get_returns_by_tickers(True)

@benchmark.register(name='[url: /returns/tickers/start_date/end_date][load test] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_tickers_load_test(state):
    while state:
        with Pool(processes=8) as pool:
            pool.map(_get_returns_by_tickers,[False for i in range(8)])

@benchmark.register(name='[url: /returns/tickers/start_date/end_date][random args/load test] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_tickers_load_test_randomize(state):
    while state:
        with Pool(processes=8) as pool:
            pool.map(_get_returns_by_tickers,[True for i in range(8)])

def _get_returns_by_date_url(randomize=False):
    if not randomize:
        return f'{YSERV_URL}/returns/{tickers}/20250811'
    else:
        start_date = get_date()
        tickers_ = get_tickers()
        url = f'{YSERV_URL}/returns/{tickers_}/{str(start_date.date())}'
        #print(url)
        return url

def _get_returns_by_date(randomize=False):
    response = requests.get(_get_returns_by_date_url(randomize))

@benchmark.register(name='[url: /returns/tickers/query_date] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_date(state):
    while state:
        _get_returns_by_date()

@benchmark.register(name='[url: /returns/tickers/query_date][random args] ')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_date(state):
    while state:
        _get_returns_by_date(randomize=True)

if __name__ == '__main__':
    benchmark.main()
