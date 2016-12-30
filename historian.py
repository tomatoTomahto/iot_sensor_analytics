from datagenerator import DataGenerator
from configparser import ConfigParser
import os

# Read config file variables
if not os.path.isfile('config.ini'):
    raise Exception('Error: missing config.ini')

config = ConfigParser()
config.read('config.ini')

dgen = DataGenerator(config)

dgen.generate_well_info()
dgen.generate_tag_mappings()
#dgen.generate_well_performance()
#dgen.generate_sensor_data()
