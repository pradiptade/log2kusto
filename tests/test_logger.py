import logging
from log2kusto.kusto_handler import KustoHandler


def send_info(log, d={}):
    logger.info(log, extra=d)

def send_warn(log, d={}):
    logger.warn(log)

def send_error(log, d={}):
    logger.error(log)

def send_critical(log, d={}):
    logger.critical(log)
class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, attributes_list=[]):
        super().__init__(fmt, datefmt, style, validate)
        self.attributes = attributes_list

    def format(self, record):
        print("in format")
        for attr in self.attributes:
            print(attr)
            if not hasattr(record, attr):
                setattr(record, attr, '')
        return super().format(record)

#https://docs.python.org/3/library/logging.html#logrecord-attributes
logrecord_attributes_list = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'message']
custom_attributes_list = ['env', 'domain']
all_attributes_list = logrecord_attributes_list + custom_attributes_list
formatter = CustomFormatter('%(' + ((')s' + " ; " + '%(').join(all_attributes_list)) + ')s', "%Y-%m-%d %H:%M:%S", \
                            attributes_list=all_attributes_list)

kusto_handler = KustoHandler('Vmainsight', 'vmadbexp', 'log_test', all_attributes_list)
kusto_handler.setLevel(logging.INFO)
kusto_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(kusto_handler)

d = {'env':'stage', 'domain':'azqualify'}
while True:
    log = input("> ")
    if log.strip().lower() != "quit":
        send_info(log, d)
        send_warn(log, d)
        send_error(log)
        send_critical(log)
    else:
        break

