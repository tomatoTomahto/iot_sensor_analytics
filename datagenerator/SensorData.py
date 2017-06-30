from DataGenerator import DataGenerator
from configparser import ConfigParser
import os, sys

def print_usage():
    print('Usage: python historian.py <config file> <option> <output>')
    print('<option> : static     : Generate well information data')
    print('         : historic   : Generate historic sensor data')
    print('         : realtime   : Generate real-time sensor data')
    print('         : all        : Generate all data')
    print('Example  : python historian.py config.ini historic')

if len(sys.argv) < 3 or sys.argv[2] not in ['static','historic','realtime','all']:
    print_usage()
    exit()

# Read config file variables
config_file = sys.argv[1]
if not os.path.isfile(config_file):
    raise Exception('Error: missing config file %s' % config_file)
config = ConfigParser()
config.read(config_file)

dgen = DataGenerator(config)

option = sys.argv[2]
if option in ['static','all']:
    dgen.generateStaticData()
if option in ['historic','all']:
    dgen.generateStaticData(load=False)
    dgen.generateSensorData()
if option in ['realtime','all']:
    dgen.generateSensorData(historic=False)
