import logging
import logging.config
import yaml
import os

default_config = """
version: 1
formatters:
   simple:
      format: '%(asctime)s %(levelname)s %(filename)s: %(message)s'
      datefmt: '%Y%m%d %H%M%S'
handlers:
   console:
      class: logging.StreamHandler
      level: DEBUG
      formatter: simple
loggers:
   XXXX:
      level: DEBUG
      handlers: [console]
      propagate: no
root:
   level: CRITICAL
   handlers: [console]
"""

class CommonLogger(logging.Logger):
    def __init__(self, name):
        logging.Logger.__init__(self, name)

logging.setLoggerClass(CommonLogger)

def create_logger(name):
    logging_dict = yaml.load(default_config, Loader=yaml.FullLoader)
    logging_dict['loggers'][name] = logging_dict['loggers']['XXXX']
    logging.config.dictConfig(logging_dict)
    return logging.getLogger(name)

logger = create_logger('default')
