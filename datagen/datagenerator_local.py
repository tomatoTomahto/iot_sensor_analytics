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
    PREVENT_MAINT = '%s sensor showing abnormal readings. Preventive maintenance required. Scheduling component replacement'
    CORRECT_MAINT = 'Asset failure due to high %s sensor and %s. Asset shutdown. Corrective maintenance required.'
    PROGRAM_COST = 20000 # Cost for doing some maintenance
    PREVENT_COST = 10000 # Cost for replacing a component
    CORRECT_COST = 200000 #

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
        self._config['predicted_entity'] = config['sensor device data']['predicted_entity']
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
        device_entities = self._config['device_entities']
        predicted_entity = self._config['predicted_entity']

        if historic:
            end_date = datetime.datetime.now()
            start_date = (end_date - dateutil.relativedelta.relativedelta(days=days_history))\
                            .replace(hour=0,minute=0,second=0,microsecond=0)
        else:
            start_date = datetime.datetime.now()
            end_date = start_date + dateutil.relativedelta.relativedelta(days=days_history)

        simulation_day = 0
        fault = False
        warning = False
        state = 0  # 0=healthy, 1=warning, 2=critical, 3=shutdown
        faulty_device = 0
        for simulation_date in [start_date + datetime.timedelta(days = x)
                     for x in range(0, days_history)]:
            end_of_day = simulation_date.replace(hour=0,minute=0,second=0,microsecond=0) + datetime.timedelta(days=1)

            simulation_day += 1
            program_maint = (simulation_day % 5 == 0)       # program maintenance every 5 days

            if state==0: # Healthy
                if random.random() <= 0.17:
                    state=1
                    faulty_device = random.randint(0, len(device_entities) - 1)
            elif state==1: # Warning
                if random.random() <= 0.50:
                    state=2
                    program_maint=False
            elif state==2: # Critical
                state=3 # Automatically shutdown state
                program_maint=False
            else: state=0 # Shutdown

            # Determine maintenance task
            # if warning and not fault: # If we had a warning yesterday
            #     if not program_maint: # If no program maintenance, then fault will occur
            #         fault = True
            #         state=2
            #     else:
            #         warning = False  # If program maintenance, then warning will be reset
            #         fault = False   # If program maintenance, then fault will be avoided
            #         state=0
            # elif warning and fault:   # If we had a warning and a fault already, reset and shutdown
            #     warning = False
            #     fault = False
            #     program_maint = False
            #     state=3
            # else:                   # If no warning yet, then generate a warning 17% of the time
            #     warning = (simulation_day % 10 == 0)
            #     if warning: state=1

            with open('sampledata/well_state.txt', 'a') as outfile:
                outfile.write('%s,%d\n' % (simulation_date.strftime('%Y-%m-%d'), state))

            num_devices = len(device_entities)

            for simulation_time in [simulation_date + datetime.timedelta(seconds = x)
                     for x in range(0, int((end_of_day-simulation_date).total_seconds()), measurement_interval)]:
                date_str = simulation_time.strftime('%Y-%m-%d %H:%M:%S')
                # Generate measurement for a random device
                raw_measurement = {}
                measurement = {}
                raw_measurement['record_time'] = date_str
                measurement['record_time'] = date_str
                if not historic:
                    num_devices = random.randint(0,len(device_entities))
                for device_id in range(0,num_devices):
                    raw_measurement['tag_id'] = int('%d' % device_id)
                    raw_measurement['value'] = random.random()*(device_id+1)+(device_id+1)*50
                    if state==3:
                        raw_measurement['value'] = 0
                    if state==2 and device_id == faulty_device:
                        raw_measurement['value'] *= 2#(1+random.random()*0.25+0.75)
                    elif state==1 and device_id == faulty_device:
                        raw_measurement['value'] *= 3#(1+random.random()*0.25+0.5)
                    measurement[device_entities[device_id]] = raw_measurement['value']
                    if historic:
                        if file:
                            with open('sampledata/raw_measurements.json','a') as outfile:
                                json.dump(raw_measurement, outfile)
                                outfile.write('\n')
                    else:
                        if file:
                            with open('sampledata/raw_measurements.json','a') as outfile:
                                json.dump(raw_measurement, outfile)
                                outfile.write('\n')
                        else:
                            self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))

                # Generate measurement for predicted device
                predicted_measurement = {}
                predicted_measurement['tag_id'] = int('%d' % len(device_entities))
                predicted_measurement['record_time'] = date_str
                predicted_measurement['value'] = 0
                for device_id in range(0,len(device_entities)):
                    predicted_measurement['value'] += measurement[device_entities[device_id]] * \
                                                      (random.random()*(device_id+1.25)+device_id*10)
                measurement[predicted_entity] = predicted_measurement['value']
                if historic:
                    if file:
                        with open('sampledata/raw_measurements.json', 'a') as outfile:
                            json.dump(predicted_measurement, outfile)
                            outfile.write('\n')
                        with open('sampledata/measurements.json','a') as outfile:
                            json.dump(measurement, outfile)
                            outfile.write('\n')
                else:
                    if file:
                        with open('sampledata/raw_measurements.json', 'a') as outfile:
                            json.dump(predicted_measurement, outfile)
                            outfile.write('\n')
                    else:
                        self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))

                if not historic:
                    time.sleep(measurement_interval)

            if program_maint or state==3:
                self._generate_maintenance_report(date=simulation_time,
                                                  state=state,
                                                  faulty_device_id=faulty_device)
                state=0 # Reset to healthy
                faulty_device = 0


    def _generate_maintenance_report(self, date, state, faulty_device_id):
        faulty_device = self._config['device_entities'][faulty_device_id]
        predicted_device = self._config['predicted_entity']

        if state==3:
            message = self.CORRECT_MAINT % (faulty_device, predicted_device)
            cost = self.PROGRAM_COST + (1+faulty_device_id)*self.PREVENT_COST + self.CORRECT_COST
        elif state==2 or state==1:
            message = self.PREVENT_MAINT % faulty_device
            cost = self.PROGRAM_COST + (1+faulty_device_id)*self.PREVENT_COST
        else:
            message = self.PROGRAM_MAINT
            cost = self.PROGRAM_COST

        with open('sampledata/maintenance_costs.csv', 'a') as outfile:
            outfile.write('%s,%d\n' % (date.strftime('%Y-%m-%d'), cost))

        with open('sampledata/maintenance_logs.txt', 'a') as outfile:
            outfile.write('%s|%s\n' % (date.strftime('%Y-%m-%d'), message))