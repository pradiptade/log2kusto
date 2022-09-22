'''

self.flush_writes() called inside self.flush()
time.sleep(100) added after self.flush_writes() call 

Idea is to pause the program while logs are being queued for Kusto ingestion, in 
case the program ends before that is completed. 

Errors: 
- can't register atexit after shutdown 

'''
print('importing KustoHandler from kusto_handler_2.py')

import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio
import time 
import atexit 
import threading
from threading import Thread

# atexit.unregister(logging.shutdown)
# atexit.register(time.sleep, 300)
# atexit.register(logging.shutdown)
# atexit.register(time.sleep, 300)

class KustoHandler(logging.Handler):
    def __init__(self, cluster:str, database:str, tablename:str, attributes_list:list) -> None:
        """Constructor
        Args:
            cluster: kusto cluster name
            database: kusto database
            table (string): table name
            attributes_list: The list of attributes that maps to the schema of the table.
        """
        super().__init__()
        
        self.cluster = cluster
        self.database = database
        self.tablename = tablename
        self.db_conn = kio.KustoIngest(kusto_ingest_cluster=self.cluster, kusto_database=self.database)
        self.attributes = attributes_list
        # self.thread = Thread(target=self.flush_writes)
        # self.threads = [] 
        # def wait_for_threads(): 
        #     while threading.active_count() > 1: 
        #         pass 
        # self.looping_thread = threading.Thread(target=wait_for_threads)
        # self.looping_thread.start()

        self.log_rows_list = []
        # self.threads = []
        return

    def emit(self, record):
        # if self.thread.is_alive():
        #     print('joining thread')
        #     self.thread.join(timeout=60)
        self.format(record)
        record_values = [record.__dict__[k] for k in self.attributes]
        self.log_rows_list.append(record_values)
        # if not self.thread.is_alive():
        #     print('running thread')
        #     self.thread.start()
        # thread = Thread(target=self.flush_writes)
        # thread = Thread(target=self.write_to_kusto, args=([record_values],))
        # self.threads.append(thread)
        # self.threads[-1].start()
        # thread.start()
        # thread.join(timeout=20)
        #print(self.log_rows_list)
        # self.threads.append(thread)

    # def write_to_kusto(self, msgs):
    #     log_df = pd.DataFrame(msgs, columns=self.attributes)
    #     self.db_conn.write_pandas_to_table(log_df, self.tablename)
    #     print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
    #         .format(len(msgs), self.cluster, self.database, self.tablename))
    #     print('Done')
        
    def flush_writes(self):
        log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        self.db_conn.write_pandas_to_table(log_df.copy(deep=True), self.tablename)
        print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
            .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))
        # self.log_rows_list = []

    def flush(self):
        pass
        print('beginning flush')
        self.flush_writes()
        # for msg in self.log_rows_list: 
        #     print(msg)

        # print('KH2 --- ', threading.active_count())
        # log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        # self.db_conn.write_pandas_to_table(log_df, self.tablename)
        # print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
        #     .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))
        # if self.thread.is_alive():
        #     self.thread.join()

        # time.sleep(100)
        # print(f'Threads: {len(self.threads)}')
        # for thread in self.threads: 
        #     print('Waiting for thread')
        #     thread.join(timeout=60)
        #     print('Finished thread')
        # self.looping_thread.join()
        # print('sleeping for a minute')
        # for thread in self.threads: 
        #     if thread.is_alive(): 
        #         thread.join()
        print('end flush')

        

    def close(self):
        super().close()





