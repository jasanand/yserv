import datetime as dt
import numpy as np

__valid_date_formats__ = ['%Y%m%d', '%Y-%m-%d', '%Y.%m.%d', '%Y/%m/%d', '%d/%m/%Y', '%Y-%m-%dD%H:%M:%S.000000000',
                          '%Y-%m-%d %H:%M:%S']
def parse_date(str_date):
    valid_date = None
    for valid_format in __valid_date_formats__:
        try:
            valid_date = dt.datetime.strptime(str_date, valid_format)
            break
        except:
            pass
    return valid_date

def today():
    return dt.datetime.combine(dt.date.today(), dt.time(0))

__offsets__ = [3, 1, 1, 1, 1, 1, 2]
def yesterday(ref_date=today(),business_day=False):
    if not business_day:
        return ref_date - dt.timedelta(days=1)
    else:
        return ref_date - dt.timedelta(days=__offsets__[ref_date.weekday()])
