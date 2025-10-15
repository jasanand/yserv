import datetime as dt
import numpy as np
from dependency_injector import providers, containers
from os import path, getcwd
import os


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

def yaml_path(yaml_filename, relative_path, _strategy_name, _location=None):
    __dir_path = path.dirname(_location) if _location else path.dirname(__file__)
    __yaml_path = path.realpath(path.join(*[__dir_path, relative_path, _strategy_name, yaml_filename]))
    if not os.path.isfile(__yaml_path):
        raise ValueError('No "%s" yaml config file found at %s' % (_strategy_name, __yaml_path))
    return __yaml_path

class ApplicationConfig:

    def __init__(self, _location=None):
        self.data = providers.Configuration()
        self.data.from_yaml(yaml_path('app_config.yaml', './', '', _location), required=True)
