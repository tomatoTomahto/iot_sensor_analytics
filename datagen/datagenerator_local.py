import random, datetime, dateutil.relativedelta, time, json
#import kudu
#from kudu.client import Partitioning
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
        self._config['kafka_topic'] = config['hadoop']['kafka_topic']
        self._config['kudu_master'] = config['hadoop']['kudu_masters']
        self._config['kudu_port'] = config['hadoop']['kudu_port']

        # Well configuration data
        self._config['wells'] = int(config['well info']['wells'])
        self._config['days_history'] = int(config['sensor device data']['days_history'])
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

        #self._connect_kafka()
        #self._connect_kudu()

    # Connect to Kafka
    def _connect_kafka(self):
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._config['kafka_brokers'],api_version=(0,9))

    # Generate well information and write to Kudu
    def generate_well_info(self):
        print 'Generating well information'
        well_info = {}

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
            with open('well_info.json', 'a') as outfile:
                json.dump(well_info, outfile)

    # Generate well sensor device to tag description mappings
    def generate_tag_mappings(self):
        print 'Generating tag ID/description mappings'
        tag_mapping = {}

        wells = self._config['wells']
        device_entities = self._config['device_entities']

        for well_id in range(1, wells+1):
            for device_id in range(0, len(device_entities)):
                tag_mapping['tag_id'] = int('%d%d' % (well_id, device_id))
                tag_mapping['well_id'] = well_id
                tag_mapping['sensor_name'] = '%s' % (device_entities[device_id])
                with open('sampledata/tag_mappings.json','a') as outfile:
                    json.dump(tag_mapping, outfile)
                    outfile.write('\n')

    # Generate well sensor data (either historic or in real-time)
    def generate_sensor_data(self, historic = False, file = False):
        print 'Generating sensor device historical data'

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
                        if file:
                            with open('sampledata/raw_measurements.json','a') as outfile:
                                json.dump(raw_measurement, outfile)
                                outfile.write('\n')
                            with open('sampledata/measurements.json','a') as outfile:
                                json.dump(measurement, outfile)
                                outfile.write('\n')
                    else:
                        if file:
                            with open('sampledata/raw_measurements.json','a') as outfile:
                                json.dump(raw_measurement, outfile)
                                outfile.write('\n')
                        else:
                            self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))

                if not historic:     
                    time.sleep(measurement_interval)
