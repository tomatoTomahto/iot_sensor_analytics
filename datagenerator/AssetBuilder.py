import csv
import random
from KuduConnection import KuduConnection

class AssetBuilder():

    # Initialize generator by reading in all config values
    def __init__(self, wells, kudu):
        self._wells = wells
        self._kudu = kudu
        self._sensor_info = {}
        self._sensors = []

    def build_wells(self, min_lat, max_lat, min_long, max_long, chemicals):
        for well_id in range(1,self._wells+1):
            well = {'well_id':well_id,
                    'latitude':random.random()*(max_lat-min_lat)+min_lat,
                    'longitude':random.random()*(max_long - min_long)+min_long,
                    'chemical':chemicals[random.randint(0,len(chemicals)-1)],
                    'depth':random.randint(50,100)}
            self._kudu.insert('impala::sensors.wells', well)
        self._kudu.flush()

    def build_assets(self, load=True):
        with open('asset_groups.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                if load:
                    self._kudu.insert('impala::sensors.asset_groups', {'group_id':int(row[0]), 'group_name':str(row[1])})
        self._kudu.flush()

        asset_groups = {}
        with open('assets.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            asset_id = 1
            for row in reader:
                asset_count = int(row[2])
                for well_id in range(1,self._wells+1):
                    for asset_number in range(1,asset_count+1):
                        if load:
                            self._kudu.insert('impala::sensors.well_assets',
                                              {'asset_id': asset_id,
                                               'well_id': well_id,
                                               'asset_group_id': int(row[0]),
                                               'asset_name': str(row[1]) + ' ' + str(asset_number)})
                        asset_groups[asset_id] = {'group_id':int(row[0]), 'well_id':well_id}
                        asset_id += 1
        self._kudu.flush()

        with open('sensors.csv', 'r') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for row in reader:
                self._sensor_info[str(row[1])] = {'group_id' : int(row[0]),
                                             'depends_on' : str(row[2]),
                                             'units' : str(row[3]),
                                             'min' : int(row[4])}

        sensors = []
        sensor_id = 1
        for asset_id in asset_groups.keys():
            for sensor_name in self._sensor_info.keys():
                if self._sensor_info[sensor_name]['group_id'] == asset_groups[asset_id]['group_id']:
                    sensor = {'sensor_id': sensor_id,
                              'asset_id': asset_id,
                              'sensor_name': sensor_name,
                              'units': self._sensor_info[sensor_name]['units']}
                    if load:
                        self._kudu.insert('impala::sensors.asset_sensors', sensor)

                    sensor['well_id'] = asset_groups[asset_id]['well_id']
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

    def build_readings(self, timestamp):
        self._scaling_factors = {}
        self._scaling_factors[0] = random.random()*0.1 + 0.95
        for sensor in self._sensors:
            sensor_id = sensor['sensor_id']
            depends_on = sensor['depends_on']
            min_value = sensor['min']
            if sensor_id not in self._scaling_factors.keys():
                self._build_dependent_readings(sensor)

                self._scaling_factors[sensor_id] = (random.random()*0.1 + 0.95) * self._scaling_factors[depends_on]

            if random.random()<0.5:
                continue

            self._kudu.insert('impala::sensors.measurements',
                              {'time': timestamp,
                               'sensor_id': sensor_id,
                               'value': min_value * self._scaling_factors[sensor_id]})

        self._kudu.flush()

    def _build_dependent_readings(self, sensor):
        if sensor['sensor_id'] == sensor['depends_on']:
            self._scaling_factors[sensor['sensor_id']] = (random.random() * 0.1 + 0.95)
            return

        sensor_id = sensor['sensor_id']
        depends_on = sensor['depends_on']
        
        if sensor['depends_on'] not in self._scaling_factors.keys():
            for dep_sensor in self._sensors:
                if dep_sensor['sensor_id'] == sensor['depends_on']:
                    self._build_dependent_readings(dep_sensor)

        self._scaling_factors[sensor_id] = (random.random() * 0.1 + 0.95) * self._scaling_factors[depends_on]
