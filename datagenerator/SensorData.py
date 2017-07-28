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
# * Connect to kafka, kudu, and spark
# * Load metadata (well info, asset info, and sensor info)
dgen = DataGenerator(config)

# # Generate static metadata about wells, assets, and sensors (COMMENT out for real-time data)
dgen.generateStaticData(load=True)

# # Generate historic sensor readings and write to Kudu (COMMENT out for real-time data)
dgen.generateSensorData(historic=True)

# # Generate real-time sensor readings and write to kafka (UNCOMMENT for real-time data)
#dgen.generateStaticData(load=False)
#dgen.generateSensorData(historic=False, kafka=True)
