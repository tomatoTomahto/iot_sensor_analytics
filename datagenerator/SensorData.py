# # Import Packages
import os, sys
sys.path.append('datagenerator')
from DataGenerator import DataGenerator
from configparser import ConfigParser

# # Read config file variables
config_file = 'config.ini'
if not os.path.isfile(config_file):
    raise Exception('Error: missing config file %s' % config_file)
config = ConfigParser()
config.read(config_file)

# # Create a data generator
dgen = DataGenerator(config)

# # Generate static metadata about wells, assets, and sensors
dgen.generateStaticData(load=False)

# # Generate historic sensor readings and write to Kudu
dgen.generateSensorData(historic=True)
