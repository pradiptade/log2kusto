import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio


DEFAULT_ATTRIBUTES_LIST = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'msg']

class KustoHandler(logging.Handler):

    def __init__(self, cluster, database, tablename ) -> None:
        super().__init__()
        
        self.cluster = cluster
        self.database = database
        self.tablename = tablename
        self.db_conn = kio.KustoIngest(kusto_ingest_cluster=self.cluster, kusto_database=self.database)
        
        self.attributes = DEFAULT_ATTRIBUTES_LIST
        self.log_frame = pd.DataFrame(columns = self.attributes)
        #self.log_frame.loc[len(self.log_frame.index)] = ['2021-08-04 15:03:51,729', 'INFO', 'test_logger.py', '<module>', 'test_logger', 'dummy']
        #print(self.log_frame)


    def emit(self, record):
        print("calling: emit")
        self.format(record)
        print(record.__dict__)
        record_values = [record.__dict__[k] for k in self.attributes]
        print(record_values)
        self.log_frame.loc[len(self.log_frame.index)] = record_values
        print(self.log_frame)

    def close(self):
        print("calling close")
        self.flush()
        del self.log_frame
        super().close()

    def flush(self):
        print("calling flush")
        print(self.log_frame)
        #self.db_conn.write_pandas_to_table(self.log_frame, self.tablename)




