#import kudu
#from kudu.client import Partitioning

class KuduConnection():
    # Initialize connection to Kudu
    def __init__(self, master, port, spark):
        self._kudu_master = master
        self._kudu_port = port
        self._spark = spark
        
    # Insert
    def batch_insert(self, table_name, records, schema):
        df = self._spark.createDataFrame(records, schema)
        df.write.format('org.apache.kudu.spark.kudu')\
          .option("kudu.master", self._kudu_master)\
          .option("kudu.table", table_name)\
          .mode("append")\
          .save()
