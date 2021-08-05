import logging
from logging_kusto.kusto_handler import KustoHandler

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


attributes_list = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'message', 'domain']
formatter = CustomFormatter('%(' + ((')s' + " ; " + '%(').join(attributes_list)) + ')s', "%Y-%m-%d %H:%M:%S", \
                            attributes_list=attributes_list)

kusto_handler = KustoHandler('Vmainsight', 'vmadbexp', 'log_test', attributes_list)
kusto_handler.setLevel(logging.INFO)
kusto_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(kusto_handler)

d = {'env':'stage', 'domain':'xyz'}
while True:
    log = input("> ")
    if log.strip().lower() != "quit":
        logger.info(log)
        logger.info(log, extra=d)
    else:
        break


