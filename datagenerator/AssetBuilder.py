import csv, random, datetime, time
from KuduConnection import KuduConnection

class AssetBuilder():

    # Initialize generator by reading in all config values
    def __init__(self, wells, kudu):
        self._wells = wells
        self._kudu = kudu
        self._sensor_info = {}
        self._sensors = []
        self._asset_ids = {}

    def get_asset_count(self):
        return len(self._asset_ids)

    def get_assets(self):
        return self._asset_ids

    def build_wells(self, min_lat, max_lat, min_long, max_long, chemicals, load=True):
        for well_id in range(1,self._wells+1):
            well = {'well_id':well_id,
                    'latitude':random.random()*(max_lat-min_lat)+min_lat,
                    'longitude':random.random()*(max_long - min_long)+min_long,
                    'chemical':chemicals[random.randint(0,len(chemicals)-1)],
                    'depth':random.randint(50,100)}
            if load:
                self._kudu.insert('impala::sensors.wells', well)
        self._kudu.flush()

    def build_assets(self, load=True):
        with open('asset_groups.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                if load:
                    self._kudu.insert('impala::sensors.asset_groups', {'group_id':int(row[0]), 'group_name':str(row[1])})
        self._kudu.flush()

        with open('assets.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            asset_id = 1
            for row in reader:
                for well_id in range(1,self._wells+1):
                    if load:
                        self._kudu.insert('impala::sensors.well_assets',
                                          {'asset_id': asset_id,
                                           'well_id': well_id,
                                           'asset_group_id': int(row[1]),
                                           'asset_name': str(row[2])})
                    self._asset_ids[asset_id] = {'asset_id':int(row[0]), 'well_id':well_id, 'asset_name':str(row[2])}
                    asset_id += 1
        self._kudu.flush()

        with open('sensors.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                self._sensor_info[str(row[1])] = {'asset_id' : int(row[0]),
                                             'depends_on' : str(row[2]),
                                             'units' : str(row[3]),
                                             'min' : int(row[4])}

        sensors = []
        sensor_id = 1
        for asset_id in self._asset_ids.keys():
            for sensor_name in self._sensor_info.keys():
                if self._sensor_info[sensor_name]['asset_id'] == self._asset_ids[asset_id]['asset_id']:
                    sensor = {'sensor_id': sensor_id,
                              'asset_id': asset_id,
                              'sensor_name': sensor_name,
                              'units': self._sensor_info[sensor_name]['units']}
                    if load:
                        self._kudu.insert('impala::sensors.asset_sensors', sensor)

                    sensor['well_id'] = self._asset_ids[asset_id]['well_id']
                    sensor['asset_id'] = asset_id
                    sensor['min'] = self._sensor_info[sensor_name]['min']
                    sensor['depends_on'] = sensor_id
                    sensors.append(sensor)
                    sensor_id += 1

        self._kudu.flush()

        for sensor in sensors:
            for dep_sensor in sensors:
                if sensor['sensor_id'] != dep_sensor['sensor_id'] and sensor['well_id'] == dep_sensor['well_id'] \
                    and self._sensor_info[sensor['sensor_name']]['depends_on'] == dep_sensor['sensor_name']:
                    sensor['depends_on'] = dep_sensor['sensor_id']
                    break

            self._sensors.append(sensor)

        print(self._sensors)

    def build_readings(self, timestamp, failed_asset=0, start_hour=0, end_hour=0):
        self._scaling_factors = {}

        for sensor in self._sensors:
            sensor_id = sensor['sensor_id']
            depends_on = sensor['depends_on']
            min_value = sensor['min']
            if sensor_id not in self._scaling_factors.keys():
                self._build_dependent_readings(sensor,
                                               datetime.datetime.fromtimestamp(timestamp).hour,
                                               failed_asset, start_hour, end_hour)

            if random.random()<0.5:
                continue

            self._kudu.insert('impala::sensors.measurements',
                              {'time': timestamp,
                               'sensor_id': sensor_id,
                               'value': min_value * self._scaling_factors[sensor_id]})

        self._kudu.flush()

    def _build_dependent_readings(self, sensor, hour, failed_asset=0, start_hour=0, end_hour=0):
        spike = 0
        alive = 1
        if sensor['asset_id'] == failed_asset and start_hour > 0 and hour >= start_hour:
            spike = 0.3
        elif end_hour > 0 and hour <= end_hour and sensor['asset_id'] == failed_asset:
            alive = 0

        if sensor['sensor_id'] == sensor['depends_on']:
            self._scaling_factors[sensor['sensor_id']] = (random.random() * 0.1 + 0.95 + spike) * alive
            return

        sensor_id = sensor['sensor_id']
        depends_on = sensor['depends_on']

        if sensor['depends_on'] not in self._scaling_factors.keys():
            for dep_sensor in self._sensors:
                if dep_sensor['sensor_id'] == sensor['depends_on']:
                    self._build_dependent_readings(dep_sensor, hour, failed_asset, start_hour, end_hour)

        self._scaling_factors[sensor_id] = (random.random() * 0.1 + 0.95 + spike) * self._scaling_factors[depends_on] * alive
