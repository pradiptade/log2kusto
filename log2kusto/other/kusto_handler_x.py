import logging
import pandas as pd
import kusto_tools.k_io.kusto_io as kio
import time
import atexit 
import asyncio

# atexit.unregister(logging.shutdown) # (3), did not work; (6), did not work; (8), did not work; (9)

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
        # atexit.register(self.flush) # (3), did not work; (6), did not work; (8), did not work 
        # atexit.register(lambda s: s.flush(), self) # (3), did not work; (6), did not work; (8), did not work 
        # atexit.register(asyncio.run, self.flush()) # (9), did not work 
        return

    def emit(self, record):
        self.format(record)
        record_values = [record.__dict__[k] for k in self.attributes]
        self.log_rows_list.append(record_values)

        # self.flush() # (5), works, but slows down program since input not accepted during flushing of previous log message
        # self.log_rows_list = [] 

        #print(self.log_rows_list)
        
    # async def flush_writes(self): # (4) did not work; (6), did not work 
    #     log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
    #     await self.db_conn.write_pandas_to_table(log_df, self.tablename) # (4), did not work; (6), did not work 
    #     print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
    #         .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))

    # async def flush_writes(self): # (7), did not work; (8), did not work 
    #     log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
    #     write_to_table = asyncio.create_task(self.db_conn\
    #         .write_pandas_to_table(log_df, self.tablename))
    #     await write_to_table
    #     print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
    #         .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))

    def flush_writes(self): 
        log_df = pd.DataFrame(self.log_rows_list, columns=self.attributes)
        self.db_conn.write_pandas_to_table(log_df, self.tablename)  
        print("\n {0} log records written to: cluster('{1}').database('{2}').{3}"\
            .format(len(self.log_rows_list), self.cluster, self.database, self.tablename))

    async def flush(self):
        print('flushing')
        pass
        
        # self.flush_writes() # (1), did not work

        # time.sleep(100) # (2), did not work 

        # event_loop = asyncio.get_event_loop() # (4), did not work; (6), did not work
        # flush_func = self.flush_writes() 
        # event_loop.run_until_complete(flush_func)

        # if self.log_rows_list is not None and len(self.log_rows_list) != 0: # (5), works, but slows down program since input not accepted during flushing of previous log message
        #     self.flush_writes()

        # asyncio.run(self.flush_writes()) # (7), did not work; (8), did not work 

        # task = asyncio.create_task(self.flush_writes()) # (9), did not work 
        # await task 

        self.flush_writes()


    def close(self):
        print('closing')
        super().close()



