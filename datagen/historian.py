from datagenerator import DataGenerator
from configparser import ConfigParser
import os, sys

def print_usage():
    print('Usage: python historian.py <config file> <option>')
    print('<option> : static     : Generate well information data')
    print('         : historic   : Generate historic sensor data')
    print('         : realtime   : Generate real-time sensor data')
    print('         : all        : Generate well info, historic & real-time sensor data')
    print('Example  : python historian.py config.ini all')

if len(sys.argv) < 3 or sys.argv[2] not in ['static','historic','realtime','all']:
    print_usage()
    exit()

# Read config file variables
config_file = sys.argv[1]
if not os.path.isfile(config_file):
    raise Exception('Error: missing config file %s' % config_file)
config = ConfigParser()
config.read('config.ini')

dgen = DataGenerator(config)

option = sys.argv[2]
if option in ['static','all']:
    dgen.create_tables()
    #dgen.generate_well_info()
    dgen.generate_tag_mappings()
if option in ['historic','all']:
    dgen.generate_sensor_data(historic=True)
if option in ['realtime','all']:
    dgen.generate_sensor_data(historic=False)
