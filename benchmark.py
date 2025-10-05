import google_benchmark as benchmark
import requests
YSERV_URL = 'http://127.0.0.1:8000'

@benchmark.register(name='get_returns_by_ticker')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_returns_by_ticker(state):
    while state:
        response = requests.get(f'{YSERV_URL}/returns/AAPL/20230101/20251003')

@benchmark.register(name='get_xs_returns_by_date')
@benchmark.option.iterations(1)
@benchmark.option.repetitions(10)
@benchmark.option.unit(benchmark.kMillisecond)
def benchmark_get_xs_returns_by_date(state):
    while state:
        response = requests.get(f'{YSERV_URL}/returns/20250811/NVDA,MSFT,AAPL,GOOG,AMZN,META,AVGO,TSLA,JPM,WMT,ORCL,LLY,V,MA,NFLX,XOM,JNJ,ABBV,PLTR,COST,HD,BAC,PG')

if __name__ == '__main__':
    benchmark.main()
