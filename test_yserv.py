import requests
from logger import logger
from io import StringIO
import math

YSERV_URL = 'http://127.0.0.1:8000'

def test_tickers():
    response = requests.get(f'{YSERV_URL}/tickers')
    assert response.status_code == 200
    #logger.info(response.json())

def test_returns():
    response = requests.get(f'{YSERV_URL}/returns/AAPL/20231004/20250926')
    assert response.status_code == 200
    returns = pd.read_json(StringIO(response.text)).set_index('date')
    assert math.isclose(returns['c2c_ret'].sum(), np.float64(0.4720421852))
    #logger.info(f'\n{returns.tail()}')

def test_returns_by_date():
    # just after div date 20250808
    response = requests.get(f'{YSERV_URL}/returns/20250811/NVDA,MSFT,AAPL,GOOG,AMZN,META')
    assert response.status_code == 200
    returns = pd.read_json(StringIO(response.text)).set_index('date')
    assert math.isclose(returns.sum(axis=1).iloc[0], np.float64(-0.0253342119))
    #logger.info(f'\n{returns}')

def test_missing_ric():
    response = requests.get(f'{YSERV_URL}/returns/AAPLXX/20231004/20250926')
    assert response.status_code == 404
    #logger.info(f'{response.json()}')

def test_invalid_date():
    response = requests.get(f'{YSERV_URL}/returns/AAPL/20230014/20250926')
    assert response.status_code == 404
    #logger.info(f'{response.json()}')

if __name__ == "__main__":
    test_tickers()
    test_returns()
    test_returns_by_date()
    test_missing_ric()
    test_invalid_date()
