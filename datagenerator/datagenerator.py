import random, datetime, dateutil.relativedelta, time, json
import kudu
from kudu.client import Partitioning
from kafka import KafkaProducer

# DataGenerator will produce the following data
# - well information - id, location, depth, chemical (oil, ng, both), type (land/offshore), leakage risk level (1-5)
# - historical well performance (todo) - daily rate (OS: $520k/day), cost ($100m/100days, L: $1-15M/100 days)
# - historical well sensor data - tag, time, value, confidence (todo)
# - real-time well sensor data - tag, time, value, confidence (todo)
class DataGenerator():
    # Initialize generator by reading in all config values
    def __init__(self, config):
        for section in ['sensor device data', 'hadoop']:
            if section not in config:
                raise Exception('Error: missing [%s] config' % (section))

        self._config = {}

        # Hadoop Config Data
        self._config['kafka_brokers'] = config['hadoop']['kafka_brokers']
        self._config['kafka_topic'] = config['hadoop']['kafka_topic']
        self._config['kudu_master'] = config['hadoop']['kudu_masters']
        self._config['kudu_port'] = config['hadoop']['kudu_port']

        # Well configuration data
        #self._config['wells'] = int(config['well info']['wells'])
        self._config['days_history'] = int(config['sensor device data']['days_history'])
        #self._config['min_long'] = int(config['well info']['min_long'])
        #self._config['max_long'] = int(config['well info']['max_long'])
        #self._config['min_lat'] = int(config['well info']['min_lat'])
        #self._config['max_lat'] = int(config['well info']['max_lat'])
        #self._config['min_well_depth'] = int(config['well info']['min_well_depth'])
        #self._config['max_well_depth'] = int(config['well info']['max_well_depth'])
        #self._config['well_types'] = config['well info']['well_types'].split(',')
        #self._config['well_chemicals'] = config['well info']['well_chemicals'].split(',')
        #self._config['max_leakage_risk'] = int(config['well info']['max_leakage_risk'])
        self._config['device_entities'] = config['sensor device data']['device_entities'].split(',')
        self._config['measurement_interval'] = int(config['sensor device data']['measurement_interval'])

        self._connect_kafka()
        self._connect_kudu()

    # Connect to Kafka
    def _connect_kafka(self):
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._config['kafka_brokers'],api_version=(0,9))

    # Connect to Kudu
    def _connect_kudu(self):
        self._kudu_client = kudu.connect(host=self._config['kudu_master'], port=self._config['kudu_port'])
        self._kudu_session = self._kudu_client.new_session()

    # Create Kudu tables
    def create_tables(self):
        for table in ['tag_mappings', 'raw_measurements', 'measurements']:
            if self._kudu_client.table_exists(table):
                self._kudu_client.delete_table(table)

        # Define a schema for a tag_mappings table
        tm_builder = kudu.schema_builder()
        tm_builder.add_column('tag_id').type(kudu.int32).nullable(False).primary_key()
        tm_builder.add_column('well_id').type(kudu.int32).nullable(False)
        tm_builder.add_column('sensor_name').type(kudu.string).nullable(False)
        tm_schema = tm_builder.build()

        # Define partitioning schema
        tm_partitioning = Partitioning().add_hash_partitions(column_names=['tag_id'], num_buckets=3)

        # Define a schema for a raw_measurements table
        rm_builder = kudu.schema_builder()
        rm_builder.add_column('record_time').type(kudu.string).nullable(False)
        rm_builder.add_column('tag_id').type(kudu.int32).nullable(False)
        rm_builder.add_column('value').type(kudu.double).nullable(False)
        rm_builder.set_primary_keys(['record_time','tag_id'])
        rm_schema = rm_builder.build()

        # Define partitioning schema
        rm_partitioning = Partitioning().add_hash_partitions(column_names=['record_time','tag_id'], 
                                                             num_buckets=3)

        tag_entities = self._config['device_entities']

        # Define a schema for a measurements table
        m_builder = kudu.schema_builder()
        m_builder.add_column('record_time').type(kudu.string).nullable(False)
        m_builder.add_column('well_id').type(kudu.int32).nullable(False)
        for entity in tag_entities:
            m_builder.add_column(entity).type(kudu.double).nullable(True)
        m_builder.set_primary_keys(['record_time','well_id'])
        m_schema = m_builder.build()

        # Define partitioning schema
        m_partitioning = Partitioning().add_hash_partitions(column_names=['record_time','well_id'], 
                                                            num_buckets=3)

        # Create new table
        self._kudu_client.create_table('tag_mappings', tm_schema, tm_partitioning, n_replicas=1)
        self._kudu_client.create_table('raw_measurements', rm_schema, rm_partitioning, n_replicas=1)
        self._kudu_client.create_table('measurements', m_schema, m_partitioning, n_replicas=1)

    # Generate well information and write to Kudu
    def generate_well_info(self):
        print 'Generating well information'
        well_info = {}
        table = self._kudu_client.table('well_info')

        wells = self._config['wells']
        min_lat = self._config['min_lat']
        max_lat = self._config['max_lat']
        min_long = self._config['min_long']
        max_long = self._config['max_long']
        min_well_depth = self._config['min_well_depth']
        max_well_depth = self._config['max_well_depth']
        well_types = self._config['well_types']
        well_chemicals = self._config['well_chemicals']
        max_leakage_risk = self._config['max_leakage_risk']

        for well_id in range(1, wells+1):
            well_info['well_id'] = well_id
            well_info['latitude'] = random.uniform(min_lat, max_lat)
            well_info['longitude'] = random.uniform(min_long, max_long)
            well_info['depth'] = random.uniform(min_well_depth, max_well_depth)
            well_info['well_type'] = well_types[random.randint(0,len(well_types)-1)]
            well_info['well_chemical'] = well_chemicals[random.randint(0,len(well_chemicals)-1)]
            well_info['leakage_risk'] = random.randint(1,max_leakage_risk)
            self._kudu_session.apply(table.new_upsert(well_info))

        self._kudu_session.flush()

    # Generate well performance information and write to Kudu
    def generate_well_performance(self):
        print 'Generating well performance data'

        # TODO - generate well performance history
        # TODO - write to Impala

    # Generate well sensor device to tag description mappings
    def generate_tag_mappings(self):
        print 'Generating tag ID/description mappings'
        tag_mapping = {}
        table = self._kudu_client.table('tag_mappings')

        wells = self._config['wells']
        device_entities = self._config['device_entities']

        for well_id in range(1, wells+1):
            for device_id in range(0, len(device_entities)):
                tag_mapping['tag_id'] = int('%d%d' % (well_id, device_id))
                tag_mapping['well_id'] = well_id
                tag_mapping['sensor_name'] = '%s' % (device_entities[device_id])
                self._kudu_session.apply(table.new_upsert(tag_mapping))

        self._kudu_session.flush()

    # Generate well sensor data (either historic or in real-time)
    def generate_sensor_data(self, historic = False):
        print 'Generating sensor device historical data'
        meas_table = self._kudu_client.table('measurements')
        raw_table = self._kudu_client.table('raw_measurements')

        days_history = self._config['days_history']
        measurement_interval = self._config['measurement_interval']
        wells = self._config['wells']
        device_entities = self._config['device_entities']

        if historic:
            end_date = datetime.datetime.now()
            start_date = end_date - dateutil.relativedelta.relativedelta(days=days_history)
        else:
            start_date = datetime.datetime.now()
            end_date = start_date + dateutil.relativedelta.relativedelta(days=days_history)

        for date in [start_date + datetime.timedelta(seconds = x)
                     for x in range(0, int((end_date-start_date).total_seconds()), measurement_interval)]:
            date_str = date.strftime('%Y-%m-%d %H:%M:%S')
            for well_id in range(1, wells+1):
                # Generate measurement for a random device
                raw_measurement = {}
                measurement = {}
                raw_measurement['record_time'] = date_str
                measurement['record_time'] = date_str
                measurement['well_id'] = well_id
                num_devices = len(device_entities)
                if not historic:
                    num_devices = random.randint(0,len(device_entities))
                for device_id in range(0,num_devices):
                    raw_measurement['tag_id'] = int('%d%d' % (well_id, device_id))
                    raw_measurement['value'] = random.random()*100+50
                    measurement[device_entities[device_id]] = raw_measurement['value']
                    if historic:
                        self._kudu_session.apply(raw_table.new_upsert(raw_measurement))
                        self._kudu_session.apply(meas_table.new_upsert(measurement))
                    else:
                        self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))
                
                self._kudu_session.flush()            

                if not historic:     
                    time.sleep(measurement_interval)
