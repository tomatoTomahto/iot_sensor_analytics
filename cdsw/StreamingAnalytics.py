from IPython.display import Image

# # Spark Streaming Demo
# ## Real-time Ingest of Sensor Data into Cloudera
# The diagram below illustrates the ways in which we can ingest sensor data into Cloudera
# A common method to pull data out of either historians (ex OSI PI, Honeywell PHD) is to 
# build an OPC UA connector. Cloudera has partnered with 3rd party OPC companies such as
# Inmation and Microsoft. These solutions provide fault-tolerant platforms that can navigate
# the firewalls and topologies of complex control system networks. 
Image('img/ingest.png')
# ## Overview of Spark Streaming
# Spark Streaming is an extension of the core Spark API that enables scalable, 
# high-throughput, fault-tolerant stream processing of live data streams. Data can be 
# ingested from many sources like Kafka, Flume, Kinesis, or TCP sockets, and can be 
# processed using complex algorithms expressed with high-level functions like map, 
# reduce, join, window and SparkSQL APIs. Finally, processed data can be pushed out to 
# filesystems, databases, and live dashboards. In fact, you can apply Spark’s machine 
# learning and graph processing algorithms on data streams.
Image('img/streaming-arch.png')
Image('img/streaming-flow.png')
Image('img/streaming-dstream-window.png')
# ## Overview of Streaming Demo
# This demo continues on the data processing from SensorAnalytics_kudu.py. Once we have
# a predictive model, we can run transformations on the data and even score it in real-time
# using Spark Streaming. After transformations, the data can be streamed into Kudu and be 
# queried instantly. 

# Spark Library Imports
from pyspark.sql import SparkSession, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import StorageLevel
import ConfigParser

# SparkSession singleton generator needed to operate on Dataframes within stream
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

spark = SparkSession.builder.appName("Realtime Sensor Analytics").getOrCreate()
sc = spark.sparkContext
sqc = SQLContext(sc)

# Read in Kudu information
config = ConfigParser.ConfigParser()
config.read('config.ini')
kuduMaster = config.get('hadoop','kudu_masters')
kuduPort = config.get('hadoop','kudu_port')
kafkaTopic = config.get('hadoop','kafka_topic')
kafkaBroker = config.get('hadoop','kafka_brokers') + ':' + '9092'

# Read in sensor and asset metadata from Kudu to join with sensor data
sensors = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kuduMaster)\
    .option('kudu.table','impala::sensors.asset_sensors').load()
  
assets = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kuduMaster)\
    .option('kudu.table','impala::sensors.well_assets').load()

sensorInfo = sensors.join(assets, ['asset_id'])

# Persist in memory for fast lookup
sensorInfo.persist(StorageLevel.MEMORY_ONLY)
sensorInfo.show()

# Initialize the Spark Streaming Context to pull data from Kafka every 5 seconds
ssc = StreamingContext(sc,30)
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafkaTopic], 
                                            {"metadata.broker.list": kafkaBroker})
sensorDS = kafkaStream.map(lambda x: x[1])\

def process(time, rdd):
    print("========= Time: %s =========" % str(time))
    try:
      rawSensor = spark.read.json(rdd)
      rawSensor = rawSensor.withColumn('sensor_id',rawSensor.sensor_id.cast('integer'))\
        .withColumn('value',rawSensor.value.cast('float'))
      
      if rawSensor.count() == 0:
        print('No data, sleep until next window')
        return

      print('Raw Sensor Data:')
      rawSensor.show(5)

      # Enrich the sensor data with metadata
      taggedSensor = rawSensor.join(sensorInfo, 'sensor_id')
      
      print ('Enriched Sensor Data')
      taggedSensor.show(5)
      
      ###
      # Performa any other transformations you want on this dataframe
      ###
      
      # Write data to Kudu
      rawSensor.write.format('org.apache.kudu.spark.kudu')\
        .option("kudu.master", kuduMaster)\
        .option("kudu.table", "impala::sensors.measurements")\
        .mode("append")\
        .save()

    except Exception as e:
        print(e)

# Every stream of records gets passed to the process() function
sensorDS.foreachRDD(process)

# Start the stream and wait until its terminated by the user
ssc.start()
ssc.awaitTermination()
