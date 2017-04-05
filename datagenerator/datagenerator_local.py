import random, datetime, dateutil.relativedelta, time, json
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
    def __init__(self, config, file):
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
        self._config['days_history'] = int(config['sensor device data']['days_history'])
        self._config['sensors'] = int(config['sensor device data']['sensors'])
        self._config['measurement_interval'] = int(config['sensor device data']['measurement_interval'])

        self._predicted_entity = 5
        self._prediction_entities = [2,4,6,8]

        print('Predicted Sensor: %d, Prediction Sensors: %s' % (self._predicted_entity, self._prediction_entities))
        self._measurements = []
        self._raw_measurements = []
        self._maintenance_costs = []
        self._maintenance_logs = []
        
        self._file = file
        if not self._file:
          import kudu
          from kudu.client import Partitioning
          self._connect_kafka()
          self._connect_kudu()

    # Connect to Kafka
    def _connect_kafka(self):
        self._kafka_producer = KafkaProducer(bootstrap_servers=self._config['kafka_brokers'],api_version=(0,10))

    # Connect to Kudu
    def _connect_kudu(self):
        self._kudu_client = kudu.connect(host=self._config['kudu_master'], port=self._config['kudu_port'])
        self._kudu_session = self._kudu_client.new_session()

    # Create Kudu tables
    def create_tables(self):
        for table in ['measurements']:
            if self._kudu_client.table_exists(table):
                self._kudu_client.delete_table(table)

        # Define a schema for a tag_mappings table
        tm_builder = kudu.schema_builder()
        tm_builder.add_column('tag_id').type(kudu.int32).nullable(False).primary_key()
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

        # Define a schema for a measurements table
        m_builder = kudu.schema_builder()
        m_builder.add_column('record_time').type(kudu.string).nullable(False)
        for device_id in range(0,self._config['sensors']):
            m_builder.add_column('Sensor_%d' % device_id).type(kudu.double).nullable(True)
        m_builder.set_primary_keys(['record_time'])
        m_schema = m_builder.build()

        # Define partitioning schema
        m_partitioning = Partitioning().add_hash_partitions(column_names=['record_time'],
                                                            num_buckets=3)

        # Create new table
        self._kudu_client.create_table('tag_mappings', tm_schema, tm_partitioning, n_replicas=3)
        self._kudu_client.create_table('raw_measurements', rm_schema, rm_partitioning, n_replicas=3)
        self._kudu_client.create_table('measurements', m_schema, m_partitioning, n_replicas=3)

    # Generate well sensor device to tag description mappings
    def generate_tag_mappings(self):
        print 'Generating tag ID/description mappings'
        tag_mapping = {}
        if not self._file:
          table = self._kudu_client.table('tag_mappings')

        for device_id in range(0, self._config['sensors']):
            tag_mapping['tag_id'] = int('%d' % device_id)
            tag_mapping['sensor_name'] = 'Sensor_%d' % device_id
            if self._file:
              with open('sampledata/tag_mappings.json','a') as outfile:
                json.dump(tag_mapping, outfile)
                outfile.write('\n')
            else:  
              self._kudu_session.apply(table.new_upsert(tag_mapping))

        if not self._file:
          self._kudu_session.flush()

    # Generate well sensor data (either historic or in real-time)
    def generate_sensor_data(self, historic = False, file = False):
        print 'Generating sensor device historical data'
        if not file:
          raw_table = self._kudu_client.table('raw_measurements')

        days_history = self._config['days_history']
        measurement_interval = self._config['measurement_interval']

        if historic:
            end_date = datetime.datetime.now()
            start_date = (end_date - dateutil.relativedelta.relativedelta(days=days_history))\
                            .replace(hour=0,minute=0,second=0,microsecond=0)
        else:
            start_date = datetime.datetime.now()
            end_date = start_date + dateutil.relativedelta.relativedelta(days=days_history)

        raw_measurements = []
        print('Simulation Duration [%s, %s]' % (start_date, end_date))

        simulation_day = 0
        state = 0   # 0=healthy, 1=warning, 2=critical, 3=shutdown
        faulty_device = 0
        for simulation_date in [start_date + datetime.timedelta(days = x)
                     for x in range(0, days_history)]:
            end_of_day = simulation_date.replace(hour=0,minute=0,second=0,microsecond=0) + datetime.timedelta(days=1)
            print(simulation_date)
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

            for simulation_time in [simulation_date + datetime.timedelta(seconds = x)
                     for x in range(0, int((end_of_day-simulation_date).total_seconds()), measurement_interval)]:
                date_str = simulation_time.strftime('%Y-%m-%d %H:%M:%S')
                if not historic:
                    print('Time: %s' % date_str)

                # Generate measurement for a random device
                raw_measurement = {}
                measurement = {}
                raw_measurement['record_time'] = date_str
                measurement['record_time'] = date_str

                for device_id in range(0,self._config['sensors']):
                    if device_id == self._predicted_entity:
                        continue

                    raw_measurement['tag_id'] = int('%d' % device_id)
                    raw_measurement['value'] = random.randint(1,4)+device_id*(5+random.random()*device_id)
                    if state==3:
                        raw_measurement['value'] = 0
                    elif state==2 and device_id == faulty_device:
                        raw_measurement['value'] *= (1.6+random.random()*0.2*random.randint(-1,1))
                    elif state==1 and device_id == faulty_device:
                        raw_measurement['value'] *= (1.3+random.random()*0.2*random.randint(-1,1))

                    raw_measurement['value'] = round(raw_measurement['value'],5)
                    measurement['Sensor_%d' % device_id] = raw_measurement['value']

                    if historic:
                        if file:
                          raw_measurements.append(json.dumps(raw_measurement))
                        else:
                          self._kudu_session.apply(raw_table.new_upsert(raw_measurement))
                    else:
                        self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(raw_measurement))

                # Generate measurement for predicted device
                predicted_measurement = {}
                predicted_measurement['tag_id'] = int('%d' % self._predicted_entity)
                predicted_measurement['record_time'] = date_str
                predicted_measurement['value'] = 0
                for device_id in self._prediction_entities:
                    predicted_measurement['value'] += measurement['Sensor_%d' % device_id] * \
                                                      (float(device_id)/self._config['sensors']) * \
                                                      (random.random()*0.2+0.9)

                if historic:
                    if file:
                      raw_measurements.append(json.dumps(predicted_measurement))
                    else:
                      self._kudu_session.apply(raw_table.new_upsert(predicted_measurement))
                else:
                    self._kafka_producer.send(self._config['kafka_topic'], value=json.dumps(predicted_measurement))
                    time.sleep(measurement_interval)

            if not file:
              self._kudu_session.flush()

            if program_maint or state==3:
                self._generate_maintenance_report(date=simulation_date,
                                                  state=state,
                                                  faulty_device_id=faulty_device)
                state=0 # Reset to healthy
                faulty_device = 0

        if file:
           print('Writing raw measurements to file')
           with open('sampledata/raw_measurements.json', 'a') as outfile:
               for measurement in raw_measurements:
                   outfile.write(measurement+'\n')

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

        cost *= (random.random()*0.3+0.7)

        duration *= (random.random()*0.4+0.6)

        # self._maintenance_costs.append('%s,%d\n' % (date.strftime('%Y-%m-%d'), cost))
        # self._maintenance_logs.append('%s|%s|%d\n' % (date.strftime('%Y-%m-%d'), message, duration))

        with open('sampledata/maint_costs.csv', 'a') as outfile:
            outfile.write('%s,%d\n' % (date.strftime('%Y-%m-%d'), cost))

        with open('sampledata/maint_notes.txt', 'a') as outfile:
            outfile.write('%s|%s|%d\n' % (date.strftime('%Y-%m-%d'), message, duration))