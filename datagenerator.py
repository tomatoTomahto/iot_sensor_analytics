import random, datetime, dateutil.relativedelta, os
import kudu
# from confluent_kafka import Producer

# Data to produce
# - well information - id, location, depth, chemical (oil, ng, both), type (land/offshore), leakage risk level (1-5)
# - historical well performance - daily rate (OS: $520k/day), cost ($100m/100days, L: $1-15M/100 days)
# - historical well sensor data - tag, time, value, confidence
# - real-time well sensor data - tag, time, value, confidence

class DataGenerator():
    def __init__(self, config):
        for section in ['well info', 'hadoop']:
            if section not in config:
                raise Exception('Error: missing [%s] config' % (section))

        self._config = {}

        # Hadoop Config Data
        self._config['hdfs_url'] = config['hadoop']['hdfs_url']
        self._config['kafka_brokers'] = config['hadoop']['kafka_brokers']
        self._config['kafka_topic'] = config['hadoop']['kafka_topic']
        self._config['kudu_master'] = config['hadoop']['kudu_masters']
        self._config['kudu_port'] = config['hadoop']['kudu_port']

        # Well configuration data
        self._config['wells'] = int(config['well info']['wells'])
        self._config['devices_per_well'] = int(config['well info']['devices_per_well'])
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
        self._config['device_measurements'] = config['sensor device data']['device_measurements'].split(',')
        self._config['measurement_interval'] = int(config['sensor device data']['measurement_interval'])

        # self._connect_kafka()
        self._connect_kudu()

    def _connect_kafka(self):
        # TODO - connect to Kafka

    def _connect_kudu(self):
        self._kudu_client = kudu.connect(host=self._config['kudu_master'], port=self._config['kudu_port'])
        self._kudu_session = self._kudu_client.new_session()

    def generate_well_info(self):
        print 'Generating well information'
        well_info = {}
        table = self._kudu_client.table('well_info')

        wells = self._config['wells']
        min_lat = self._config['min_lat']
        max_lat = self.config['max_lat']
        min_long = self._config['min_long']
        max_long = self._config['max_long']
        min_well_depth = self._config['min_well_depth']
        max_well_depth = self._config['max_well_depth']
        well_types = self._config['well_types']
        well_chemicals = self._config['well_chemicals']
        max_leakage_risk = self._config['max_leakage_risk']

        for well_id in range(1, wells):
            well_info['latitude'] = random.uniform(min_lat, max_lat)
            well_info['longitude'] = random.uniform(min_long, max_long)
            well_info['depth'] = random.uniform(min_well_depth, max_well_depth)
            well_info['well_type'] = well_types[random.randint(0,len(well_types)-1)]
            well_info['well_chemical'] = well_chemicals[random.randint(0,len(well_chemicals)-1)]
            well_info['leakage_risk'] = random.randint(1,max_leakage_risk)
            table.new_insert(well_info)

    def generate_well_performance(self):
        print 'Generating well performance data'
        well_performance = pd.DataFrame(columns = ['well_id','date','output','cost'])

        # TODO - generate well performance history
        # TODO - write to Impala

    def generate_tag_mappings(self):
        print 'Generating tag ID/description mappings'
        tag_mapping = {}
        table = self._kudu_client.table('tag_mappings')

        wells = self._config['wells']
        devices_per_well = self.config['devices_per_well']
        device_entities = self._config['device_entities']
        device_measurements = self._config['device_measurements']

        for well_id in range(1, wells):
            for device_id in range(1, devices_per_well):
                tag_mapping['tag_id'] = '%i%i000000' % (well_id, device_id)
                tag_mapping['tag_entity'] = 'Well %i' % (well_id)
                tag_mapping['tag_description'] = 'Well %i %s %i %s' % \
                                                 (well_id,
                                                  device_entities[random.randint(0, len(device_entities) - 1)],
                                                  device_id,
                                                  device_measurements[random.randint(0, len(device_measurements) - 1)])
                table.new_insert(tag_mapping)

    def generate_sensor_data(self, historical = False):
        print 'Generating sensor device historical data'
        sensor_data = pd.DataFrame(columns = ['tag_id','ticks','value'])

        months_history = self._config['months_history']
        measurement_interval = self._config['measurement_interval']
        wells = self._config['wells']
        devices_per_well = self._config['devices_per_well']

        if historical:
            end_date = datetime.datetime.now()
            start_date = end_date - dateutil.relativedelta.relativedelta(months=months_history)
        else:
            start_date = datetime.datetime.now()
            end_date = start_date + dateutil.relativedelta.relativedelta(months=months_history)

        for date in [start_date + datetime.timedelta(seconds = x)
                     for x in range(0, int((end_date-start_date).total_seconds()), measurement_interval)]:
            date_str = date.strftime('%Y-%m-%d %H:%M:%S')
            for well_id in range(1, wells):
                for device_id in range(1, devices_per_well):
                    tag_id = '%i%i000000' % (well_id, device_id)
                    value = random.uniform(50,100)
                    sensor_data.append(tag_id, date_str, value)

                # TODO - write to Kafka