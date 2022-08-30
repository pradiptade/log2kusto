import logging
from log2kusto.kusto_handler import KustoHandler

# each test case is a tuple with the following values:
# cluster, database, table, and boolean value for if an Exception should be raised
TEST_CASES = [ 
    ('https://ingest-vmainsight.kusto.windows.net/', 'vmadbexp', 'log_test', 
        False), 
    ('https://ingest-vmainsight.kusto.windows.net/', 'vmadbexp', 
        'nonexistent_table', True),
]

def test_logger_single_case(cluster, db, table):
    def get_extra_field(keyname, val):
        return {keyname:val}

    def send_info(log:str, dimensions:str = ''):
        d = get_extra_field("dimensions", dimensions )
        logger.info(log, extra=d)

    def send_warn(log:str, dimensions:str = ''):
        d = get_extra_field("dimensions", dimensions )
        logger.warning(log, extra=d)

    def send_error(log:str, dimensions:str = ''):
        d = get_extra_field("dimensions", dimensions )
        logger.error(log, extra=d)

    def send_critical(log:str, dimensions:str = ''):
        d = get_extra_field("dimensions", dimensions )
        logger.critical(log, extra=d)
    class CustomFormatter(logging.Formatter):
        def __init__(self, fmt=None, datefmt=None, style='%', validate=True, attributes_list=[]):
            super().__init__(fmt, datefmt, style, validate)
            self.attributes = attributes_list

        def format(self, record):
            #print("in format")
            for attr in self.attributes:
                #print(attr)
                if not hasattr(record, attr):
                    setattr(record, attr, '')
            return super().format(record)

    #https://docs.python.org/3/library/logging.html#logrecord-attributes
    logrecord_attributes_list = ['asctime', 'levelname', 'funcName', 'module', 'message']
    custom_attributes_list = ['dimensions', 'project_name']
    all_attributes_list = logrecord_attributes_list + custom_attributes_list
    formatter = CustomFormatter('%(' + ((')s' + " ; " + '%(').join(all_attributes_list)) + ')s', "%Y-%m-%d %H:%M:%S", \
                                attributes_list=all_attributes_list)

    kusto_handler = KustoHandler(cluster, db, table, all_attributes_list)
    kusto_handler.setLevel(logging.INFO)
    kusto_handler.setFormatter(formatter)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if len(logger.handlers) == 1: # remove KustoHandler from previous test
        logger.removeHandler(logger.handlers[0])
    logger.addHandler(kusto_handler)

    #logger = logging.LoggerAdapter(logger, {"project_name":"test"})

    while True:
        log = input("> ")
        if log.strip().lower() != "quit":
            send_info(log, "extra message")
            send_warn(log, "key1:val1, key2:val2")
            send_error(log)
            send_critical(log, "exception")
        else:
            break

    logger.handlers[0].flush_writes()

def test_logger(): 
    # test logger against every test case 
    test_results = []
    for i in range(len(TEST_CASES)):
        cluster, db, table, exception = TEST_CASES[i]
        try: 
            print(f'TEST CASE {i + 1} - Cluster: {cluster}, Database: {db}'
                + f' Table: {table}\n')
            test_logger_single_case(cluster, db, table)
            if not exception: 
                print(f'\nTest case {i + 1} passed, Exception not thrown\n')
                test_results.append(1)
            else: 
                print(f'\nTest case {i + 1} failed, Exception should have '
                    + 'been thrown\n')
                test_results.append(0)
        except: 
            if exception: 
                print(f'\nTest case {i + 1} passed, Exception thrown\n')
                test_results.append(1)
            else: 
                print(f'\nTest case {i + 1} failed, Exception should not have '
                    + 'been thrown\n')
                test_results.append(0)

    # print test summary 
    print('Test case summary:')
    for i in range(len(TEST_CASES)):
        res = 'PASSED' if test_results[i] == 1 else 'FAILED'
        print(f'Test case {i + 1} (Cluster: {cluster}, Database: {db}'
            + f' Table: {table}) - {res}')

if __name__=='__main__':
    test_logger()
