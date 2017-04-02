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
    PROGRAM_MAINT = 'Program maintenance tests all passed. Asset healthy. Sensor readings normal'
    PREVENT_MAINT = '%s showing abnormal readings. Preventive maintenance required. Scheduling component replacement'
    CORRECT_MAINT = 'Asset failure due to high %s and Sensor_%s. Asset shutdown. Corrective maintenance required.'
    PROGRAM_COST = 20000 # Cost for doing some maintenance
    PREVENT_COST = 1000 # Cost for replacing a component
    CORRECT_COST = 50000 # Cost for shutdown
    PROGRAM_DUR = 4 # Hours for a program maintenance
    PREVENT_DUR = 10
    CORRECT_DUR = 24

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
        self._config['sensors'] = int(config['sensor device data']['sensors'])
        self._config['measurement_interval'] = int(config['sensor device data']['measurement_interval'])

        self._predicted_entity = random.randint(0,self._config['sensors']-1)
        self._prediction_entities = random.sample(range(0, self._config['sensors']-1), 5)
        if self._predicted_entity in self._prediction_entities:
            self._prediction_entities.remove(self._predicted_entity)

        print('Predicted Sensor: %d, Prediction Sensors: %s' % (self._predicted_entity, self._prediction_entities))
        self._measurements = []
        self._raw_measurements = []
        self._maintenance_costs = []
        self._maintenance_logs = []

        #self._connect_kafka()
        #self._connect_kudu()

    # Connect to Kafka
    def _connect_kafka(self):
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._config['kafka_brokers'],api_version=(0,9))

    # Generate well sensor device to tag description mappings
    def generate_tag_mappings(self):
        print 'Generating tag ID/description mappings'
        tag_mapping = {}

        wells = self._config['wells']

        for well_id in range(1, wells+1):
            for device_id in range(0, self._config['sensors']):
                tag_mapping['tag_id'] = int('%d' % device_id)
                tag_mapping['well_id'] = well_id
                tag_mapping['sensor_name'] = 'Sensor_%d' % device_id
                with open('sampledata/tag_mappings.json','a') as outfile:
                    json.dump(tag_mapping, outfile)
                    outfile.write('\n')

    # Generate well sensor data (either historic or in real-time)
    def generate_sensor_data(self, historic = False, file = False):
        print 'Generating sensor device historical data'

        days_history = self._config['days_history']
        measurement_interval = self._config['measurement_interval']

        if historic:
            end_date = datetime.datetime.now()
            start_date = (end_date - dateutil.relativedelta.relativedelta(days=days_history))\
                            .replace(hour=0,minute=0,second=0,microsecond=0)
        else:
            start_date = datetime.datetime.now()
            end_date = start_date + dateutil.relativedelta.relativedelta(days=days_history)

        simulation_day = 0
        state = 0   # 0=healthy, 1=warning, 2=critical, 3=shutdown
        faulty_device = 0
        for simulation_date in [start_date + datetime.timedelta(days = x)
                     for x in range(0, days_history)]:
            end_of_day = simulation_date.replace(hour=0,minute=0,second=0,microsecond=0) + datetime.timedelta(days=1)

            simulation_day += 1
            program_maint = (simulation_day % 5 == 0 and state==0)       # program maintenance every 5 days

            if state==0: # Healthy
                if random.random() <= 0.20:
                    state=1 # Warning
                    faulty_device = self._prediction_entities[random.randint(0, len(self._prediction_entities)-1)]
            elif state==1:
                state=2 # Critical
                program_maint=False
            else:
                state=3 # Shutdown
                program_maint=False
            #else: state=0 # Reset

            #print('%s | %d' % (simulation_date.strftime('%Y-%m-%d'), state))


            for simulation_time in [simulation_date + datetime.timedelta(seconds = x)
                     for x in range(0, int((end_of_day-simulation_date).total_seconds()), measurement_interval)]:
                date_str = simulation_time.strftime('%Y-%m-%d %H:%M:%S')
                # Generate measurement for a random device
                raw_measurement = {}
                measurement = {}
                raw_measurement['record_time'] = date_str
                measurement['record_time'] = date_str

                for device_id in range(0,self._config['sensors']):
                    if device_id == self._predicted_entity:
                        continue

                    raw_measurement['tag_id'] = int('%d' % device_id)
                    raw_measurement['value'] = 1+device_id*(5+random.random()*device_id)
                    if state==3:
                        raw_measurement['value'] = 0
                    elif state==2 and device_id == faulty_device:
                        raw_measurement['value'] *= (1.6+random.random()*0.2*random.randint(-1,1))
                    elif state==1 and device_id == faulty_device:
                        raw_measurement['value'] *= (1.3+random.random()*0.2*random.randint(-1,1))

                    raw_measurement['value'] = round(raw_measurement['value'],5)
                    measurement['Sensor_%d' % device_id] = raw_measurement['value']

                    if historic:
                        self._raw_measurements.append(json.dumps(raw_measurement))
                        #print('%s|%d' % (date_str, device_id))
                    else:
                        self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))

                # Generate measurement for predicted device
                predicted_measurement = {}
                predicted_measurement['tag_id'] = int('%d' % self._predicted_entity)
                predicted_measurement['record_time'] = date_str
                    #(simulation_time + datetime.timedelta(seconds=measurement_interval))\
                    #.strftime('%Y-%m-%d %H:%M:%S')
                predicted_measurement['value'] = 0
                for device_id in self._prediction_entities:
                    predicted_measurement['value'] += measurement['Sensor_%d' % device_id] * \
                                                      (float(device_id)/self._config['sensors']) * \
                                                      (random.random()*0.2+0.9)

                if historic:
                    self._raw_measurements.append(json.dumps(predicted_measurement))
                    #if state==3:
                        #print('  %s | %d' % (date_str, predicted_measurement['value']))
                        #print('   %s' % json.dumps(predicted_measurement))
                else:
                    self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))
                    time.sleep(measurement_interval)

            if program_maint or state==3:
                self._generate_maintenance_report(date=simulation_date,
                                                  state=state,
                                                  faulty_device_id=faulty_device)
                state=0 # Reset to healthy
                faulty_device = 0

        print('Writing raw measurements to file')
        with open('sampledata/raw_measurements.json', 'a') as outfile:
            for measurement in self._raw_measurements:
                outfile.write(measurement+'\n')
        # print('Writing measurements to file')
        # with open('sampledata/measurements.json', 'a') as outfile:
        #     for measurement in self._measurements:
        #         outfile.write(measurement+'\n')
        print('Writing maintenance costs to file')
        with open('sampledata/maintenance_costs.csv', 'a') as outfile:
            for maint_cost in self._maintenance_costs:
                outfile.write(maint_cost)
        print('Writing maintenance logs to file')
        with open('sampledata/maintenance_logs.csv', 'a') as outfile:
            for maint_log in self._maintenance_logs:
                outfile.write(maint_log)

    def _generate_maintenance_report(self, date, state, faulty_device_id):
        faulty_device = 'Sensor_%d' % faulty_device_id
        predicted_device = self._predicted_entity
        cost = self.PROGRAM_COST
        duration = self.PROGRAM_DUR

        if state==3:
            message = self.CORRECT_MAINT % (faulty_device, predicted_device)
            cost += (1+faulty_device_id)*self.PREVENT_COST + self.CORRECT_COST
            duration = self.CORRECT_DUR
        elif state==2 or state==1:
            message = self.PREVENT_MAINT % faulty_device
            cost += (1+faulty_device_id)*self.PREVENT_COST
            duration = self.PREVENT_DUR
        else:
            message = self.PROGRAM_MAINT

        cost *= (random.random()*0.3+0.85)

        duration *= (random.random()*0.4+0.8)

        self._maintenance_costs.append('%s,%d\n' % (date.strftime('%Y-%m-%d'), cost))
        self._maintenance_logs.append('%s|%s|%d\n' % (date.strftime('%Y-%m-%d'), message, duration))

        # with open('sampledata/maintenance_costs.csv', 'a') as outfile:
        #     outfile.write('%s,%d\n' % (date.strftime('%Y-%m-%d'), cost))
        #
        # with open('sampledata/maintenance_logs.txt', 'a') as outfile:
        #     outfile.write('%s|%s|%d\n' % (date.strftime('%Y-%m-%d'), message, duration))