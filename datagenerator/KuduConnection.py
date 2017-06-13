import kudu
from kudu.client import Partitioning

class KuduConnection():
    # Initialize connection to Kudu
    def __init__(self, master, port):
        self._kudu_client = kudu.connect(host=master, port=port)
        self._kudu_session = self._kudu_client.new_session()

    # Create a Kudu table
    def create_table(self, table_name, schema, partition_columns, buckets, replicas=3):
        if self._kudu_client.table_exists(table_name):
            self._kudu_client.delete_table(table_name)

        # Define a schema for a tag_mappings table
        tm_builder = kudu.schema_builder()
        for column in schema:
            tm_builder.add_column(column['name']).type(column['type']).nullable(False)

        tm_schema = tm_builder.build()

        # Define partitioning schema
        tm_partitioning = Partitioning().add_hash_partitions(column_names=partition_columns, num_buckets=buckets)

        # Create the table
        self._kudu_client.create_table(table_name, tm_schema, tm_partitioning, replicas)

    # Insert
    def insert(self, table_name, record):
        table = self._kudu_client.table(table_name)
        self._kudu_session.apply(table.new_insert(record))

    # Upsert
    def upsert(self, table_name, record):
        table = self._kudu_client.table(table_name)
        self._kudu_session.apply(table.new_upsert(record))

    # Flush
    def flush(self):
        self._kudu_session.flush()
