from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.streaming.kafka import KafkaUtils
from configparser import ConfigParser
import json
from kafka import KafkaProducer

# Parse out config fields
config = ConfigParser()
config.read('config.ini')
kafka_brokers = config['hadoop']['kafka_brokers']
kafka_topic_src = config['hadoop']['kafka_topic_src']
kafka_topic_tgt = config['hadoop']['kafka_topic_tgt']
kudu_master = config['hadoop']['kudu_masters']
kudu_port = config['hadoop']['kudu_port']
interval = int(config['sensor device data']['measurement_interval'])

# SparkSession singleton generator needed to operate on Dataframes
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

# Send messages to Kafka
def sendKafka(messages):
    producer = KafkaProducer(bootstrap_servers=kafka_brokers,api_version=(0,9))
    for message in messages:
        yield producer.send(kafka_topic_tgt, value=str(message))
    producer.flush()

# Initialize SparkSession variable and Spark/Streaming/SQL Contexts
spark = SparkSession\
    .builder\
    .appName("Streaming Historian Data")\
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, interval)
sqc = SQLContext(sc, spark)

# Turn off INFO/WARN logs
sc.setLogLevel('FATAL')

# Read in Well information from Kudu to join with sensor data
well_info = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kudu_master)\
    .option('kudu.table','well_info')\
    .load()

# Read in Tag ID/Entity/Well mappings from Kudu to join with sensor data
tag_mappings = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kudu_master)\
    .option('kudu.table','well_tags')\
    .load()\
    .join(well_info, 'well_id')

# Persist in memory for fast lookup
tag_mappings.persist()
tag_mappings.collect()
tag_mappings.show()

# Read in sensor data from Kafka at 10 second intervals with a 20 second rolling window
kafkaStream = KafkaUtils.createDirectStream(ssc, [kafka_topic_src], {"metadata.broker.list": kafka_brokers})
sensorDS = kafkaStream.map(lambda x: x[1])\
    .window(3*interval,interval)

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Instantiate Kafka producer
        producer = KafkaProducer(bootstrap_servers=kafka_brokers,api_version=(0,9))

        # Read in the raw data and convert timestamp to a string integer
        rawSensor = spark.read.json(rdd)
        #rawSensor.show()

        # Join the sensor data with tag ID mappings
        taggedSensor = rawSensor.join(tag_mappings, 'tag_id', 'inner')\
            .withColumn('id',concat(rawSensor.record_time, tag_mappings.well_id.cast('string'), tag_mappings.tag_entity.cast('string')))\
            .withColumn('record_time_solr', regexp_replace('record_time', ' ', 'T'))\
            .withColumn('record_time_solr',concat('record_time_solr',lit('Z')))

        # Send this data to Kafka to be loaded into Search
        taggedSensor.toJSON().foreachPartition(sendKafka)
        #taggedSensor.show()

        # Cast all columns to the right data types (record_time:bigint, tag_id:int, value:float)
        timeSensor = taggedSensor.withColumn('record_time', regexp_replace('record_time', '[-: ]+', ''))
        castSensor = timeSensor.select(timeSensor.record_time.cast('bigint'), 
                                       timeSensor.tag_id.cast('integer'), 
                                       timeSensor.value.cast('float'),
                                       timeSensor.well_id,
                                       timeSensor.tag_entity)
       # castSensor.show()

        # Rejoin the data with tag entity and well mappings
        fullSensor = castSensor.join(tag_mappings, ['tag_id','well_id','tag_entity'], 'right_outer')
        #fullSensor.show()

        # Pivot the data to show 1 column for each tag entity
        finalSensor = fullSensor.groupBy('record_time','well_id')\
            .pivot('tag_entity')\
            .agg(sum('value'))\
            .filter('record_time is not null')
        #finalSensor.show()

        # Write back to Kafka
        finalSensor.toJSON().foreachPartition(sendKafka)
        
    except Exception as e:
        print(e)
        #pass

sensorDS.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
