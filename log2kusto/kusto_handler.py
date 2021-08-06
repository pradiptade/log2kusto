import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio


class KustoHandler(logging.Handler):
    def __init__(self, cluster, database, tablename, attributes_list) -> None:
        super().__init__()
        
        self.cluster = cluster
        self.database = database
        self.tablename = tablename
        self.db_conn = kio.KustoIngest(kusto_ingest_cluster=self.cluster, kusto_database=self.database)
        self.attributes = attributes_list

        self.log_rows_list = []

    def emit(self, record):
        self.format(record)
        record_values = [record.__dict__[k] for k in self.attributes]
        self.log_rows_list.append(record_values)
        
    def flush(self):
        log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        self.db_conn.write_pandas_to_table(log_df, self.tablename)
        print("\n Logs written to: cluster('{0}').database('{1}').{2}".format(self.cluster, self.database,self.tablename))

    def close(self):
        super().close()



