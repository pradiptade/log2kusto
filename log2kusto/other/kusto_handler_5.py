'''

self.flush_writes() called inside self.flush()
Call self.flush() in self.emit(), clear self.log_rows_list, add check to 
self.flush() to check if self.log_rows_list is empty before queueing ingestion

Errors: 
- Works, but slows down program by queueing ingestion for every log message, 
takes time away from accepting new log messages 

'''
print('importing KustoHandler from kusto_handler_5.py')

import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio


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
        return

    def emit(self, record):
        self.format(record)
        record_values = [record.__dict__[k] for k in self.attributes]
        self.log_rows_list.append(record_values)
        self.flush()
        self.log_rows_list = []
        #print(self.log_rows_list)
        
    def flush_writes(self):
        log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        self.db_conn.write_pandas_to_table(log_df, self.tablename)
        print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
            .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))

    def flush(self):
        pass
        if self.log_rows_list is not None and len(self.log_rows_list) != 0:
            self.flush_writes()

    def close(self):
        super().close()



