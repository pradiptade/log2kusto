import logging
from logging_kusto.kusto_handler import KustoHandler

# logging.basicConfig(
#             format='%(asctime)s, %(levelname)s, \"%(message)s\"', 
#             level=logging.INFO, 
#             datefmt='%m/%d/%Y %I:%M:%S %p'
#             )

attributes_list = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'msg']
formatter = logging.Formatter('%(' + ((')s' + "|" + '%(').join(attributes_list)) + ')s')

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

kusto_handler = KustoHandler(cluster='Vmainsight', database='vmadbexp', tablename='log_test')
kusto_handler.setLevel(logging.INFO)
kusto_handler.setFormatter(formatter)

logger.addHandler(kusto_handler)

log = ">"
while log.strip().lower() != "quit":
    log = input("> ")
    logger.info(log)

logger.handlers[0].close()