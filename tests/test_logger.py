import logging
from logging_kusto.kusto_handler import KustoHandler


# logging.basicConfig(
#             format='%(asctime)s, %(levelname)s, \"%(message)s\"', 
#             level=logging.INFO, 
#             datefmt='%m/%d/%Y %I:%M:%S %p'
#             )

attributes_list = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'msg', 'message', 'env']
### %(asctime)s;%(levelname)s;%(filename)s;%(funcName)s;%(module)s;%(msg)s
formatter = logging.Formatter('%(' + ((')s' + " ; " + '%(').join(attributes_list)) + ')s', "%Y-%m-%d %H:%M:%S")

kusto_handler = KustoHandler(cluster='Vmainsight', database='vmadbexp', tablename='log_test')
kusto_handler.setLevel(logging.INFO)
kusto_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(kusto_handler)

d = {'env':'stage', 'domain':'xyz'}
while True:
    log = input("> ")
    if log.strip().lower() != "quit":
        #logger.info(log)
        logger.info(log, extra=d)
    else:
        break


