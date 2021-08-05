import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio


DEFAULT_ATTRIBUTES_LIST = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'msg', 'message', 'env']
class KustoHandler(logging.Handler):

    def __init__(self, cluster, database, tablename ) -> None:
        super().__init__()
        
        self.cluster = cluster
        self.database = database
        self.tablename = tablename
        self.db_conn = kio.KustoIngest(kusto_ingest_cluster=self.cluster, kusto_database=self.database)
        
        self.attributes = DEFAULT_ATTRIBUTES_LIST
        self.log_rows_list = []

    def emit(self, record):
        print("calling: emit")
        self.format(record)
        print(record.__dict__)
        record_values = [record.__dict__[k] for k in self.attributes]
        print(record_values)
        self.log_rows_list.append(record_values)
        print("Record List:")
        print(self.log_rows_list)

    def close(self):
        #print("calling close")
        super().close()

    def flush(self):
        #print("calling flush")
        log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        print(log_df)
        #self.db_conn.write_pandas_to_table(log_df, self.tablename)




