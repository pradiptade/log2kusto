import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio
import threading
from threading import Thread
import time

print('importing KustoHandler from kusto_handler_1.py')

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

        self.log_rows_list = []

        def write_to_kusto(self):
            while True: 
                if len(self.log_rows_list) != 0: 
                    print(self.log_rows_list)
                    msg = self.log_rows_list.pop(0)
                    log_df = pd.DataFrame([msg], columns=self.attributes)
                    self.db_conn.write_pandas_to_table(log_df, self.tablename)
                    print(f'Writing {msg} to Kusto...')
                if threading.active_count() == 1: 
                    break

        write_thread = Thread(target=write_to_kusto, args=(self,))
        write_thread.start()
        return

    def emit(self, record):
        self.format(record)
        record_values = [record.__dict__[k] for k in self.attributes]
        self.log_rows_list.append(record_values)
        #print(self.log_rows_list)

    def wait_for_thread_completion(self):
        while len(self.log_rows_list) != 0: 
            print(self.log_rows_list)
            time.sleep(10)
        
    # def flush_writes(self):
    #     log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
    #     self.db_conn.write_pandas_to_table(log_df, self.tablename)
    #     print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
    #         .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))

    def flush(self):
        pass
        #self.flush_writes()
        # log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        # self.db_conn.write_pandas_to_table(log_df, self.tablename)
        # print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
        #     .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))
        # while len(self.log_rows_list) != 0: 
        #     print(self.log_rows_list)
        #     time.sleep(10)
        self.wait_for_thread_completion()

    def close(self):
        super().close()



