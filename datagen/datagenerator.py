import random, datetime, dateutil.relativedelta, time, json
import kudu
from kafka import KafkaProducer

# DataGenerator will produce the following data
# - well information - id, location, depth, chemical (oil, ng, both), type (land/offshore), leakage risk level (1-5)
# - historical well performance (todo) - daily rate (OS: $520k/day), cost ($100m/100days, L: $1-15M/100 days)
# - historical well sensor data - tag, time, value, confidence (todo)
# - real-time well sensor data - tag, time, value, confidence (todo)
class DataGenerator():
    # Initialize generator by reading in all config values
    def __init__(self, config):
        for section in ['well info', 'hadoop']:
            if section not in config:
                raise Exception('Error: missing [%s] config' % (section))

        self._config = {}

        # Hadoop Config Data
        self._config['kafka_brokers'] = config['hadoop']['kafka_brokers']
        self._config['kafka_topic_src'] = config['hadoop']['kafka_topic_src']
        self._config['kafka_topic_tgt'] = config['hadoop']['kafka_topic_tgt']
        self._config['kudu_master'] = config['hadoop']['kudu_masters']
        self._config['kudu_port'] = config['hadoop']['kudu_port']

        # Well configuration data
        self._config['wells'] = int(config['well info']['wells'])
        self._config['months_history'] = int(config['sensor device data']['months_history'])
        self._config['min_long'] = int(config['well info']['min_long'])
        self._config['max_long'] = int(config['well info']['max_long'])
        self._config['min_lat'] = int(config['well info']['min_lat'])
        self._config['max_lat'] = int(config['well info']['max_lat'])
        self._config['min_well_depth'] = int(config['well info']['min_well_depth'])
        self._config['max_well_depth'] = int(config['well info']['max_well_depth'])
        self._config['well_types'] = config['well info']['well_types'].split(',')
        self._config['well_chemicals'] = config['well info']['well_chemicals'].split(',')
        self._config['max_leakage_risk'] = int(config['well info']['max_leakage_risk'])
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
        table = self._kudu_client.table('well_tags')

        wells = self._config['wells']
        device_entities = self._config['device_entities']

        for well_id in range(1, wells+1):
            for device_id in range(0, len(device_entities)):
                tag_mapping['tag_id'] = int('%d%d' % (well_id, device_id))
                tag_mapping['well_id'] = well_id
                tag_mapping['tag_entity'] = '%s' % (device_entities[device_id])
                self._kudu_session.apply(table.new_upsert(tag_mapping))

        self._kudu_session.flush()

    # Generate well sensor data (either historic or in real-time)
    def generate_sensor_data(self, historic = False):
        print 'Generating sensor device historical data'

        months_history = self._config['months_history']
        measurement_interval = self._config['measurement_interval']
        wells = self._config['wells']
        device_entities = self._config['device_entities']

        if historic:
            end_date = datetime.datetime.now()
            start_date = end_date - dateutil.relativedelta.relativedelta(months=months_history)
        else:
            start_date = datetime.datetime.now()
            end_date = start_date + dateutil.relativedelta.relativedelta(months=months_history)

        for date in [start_date + datetime.timedelta(seconds = x)
                     for x in range(0, int((end_date-start_date).total_seconds()), measurement_interval)]:
            date_str = date.strftime('%Y-%m-%d %H:%M:%S')
            for well_id in range(1, wells+1):
                for device_id in range(0, len(device_entities)):
                    payload = {}
                    payload['record_time'] = date_str
                    payload['tag_id'] = int('%d%d' % (well_id, device_id))
                    payload['value'] = random.uniform(50,100)
                    #payload['message'] = '%s,%d,%f' % (date_str,tag_id,value)

                    self._kafka_producer.send(self._config['kafka_topic_src'], value=json.dumps(payload))
                    self._kafka_producer.send(self._config['kafka_topic_tgt'], value=json.dumps(payload))
                    #print(json.dumps(payload))
            
            if not historic:     
                time.sleep(measurement_interval)
