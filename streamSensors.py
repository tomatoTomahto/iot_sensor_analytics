from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# SparkSession singleton generator needed to operate on Dataframes
def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

# Initialize SparkSession variable and Spark/Streaming/SQL Contexts
spark = SparkSession\
    .builder\
    .appName("Streaming Historian Data")\
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 10)
sqc = SQLContext(sc, spark)

# Turn off INFO/WARN logs
sc.setLogLevel('FATAL')

# Read in Tag ID/Entity/Well mappings from Kudu to join with sensor data
tag_mappings = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master','10.0.0.48')\
    .option('kudu.table','tag_mappings')\
    .load()

# Persist in memory for fast lookup
tag_mappings.persist()
tag_mappings.collect()
tag_mappings.show()

# Read in sensor data from Kafka at 10 second intervals with a 20 second rolling window
kafkaStream = KafkaUtils.createDirectStream(ssc, [historian], {"metadata.broker.list": '10.0.0.134:9092'})
sensorDS = kafkaStream.map(lambda x: x[1].split(","))\
    .window(20,10)

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())

        # Read in the raw data and convert timestamp to a string integer
        rawSensor = spark.createDataFrame(rdd)\
            .toDF('record_time', 'tag_id', 'value')\
            .withColumn('record_time', regexp_replace('record_time', '[-: ]+', ''))

        # Cast all columns to the right data types (record_time:bigint, tag_id:int, value:float)
        castSensor = rawSensor.select(rawData.record_time.cast('bigint'), 
                                      rawData.tag_id.cast('integer'), 
                                      rawData.value.cast('float'))

        # Join the sensor data with tag ID mappings
        taggedSensor = rawSensor.join(tag_mappings, 'tag_id', 'inner')

        # Rejoin the data with tag entity and well mappings
        fullSensor = tempSensor.join(tag_mappings, ['tag_id','well','tag_entity'], 'right_outer')

        # Pivot the data to show 1 column for each tag entity
        finalSensor = fullSensor.groupBy('measurement_time','well')\
            .pivot('tag_entity')\
            .agg(sum('value'))\
            .filter('record_time is not null')
        finalSensor.show()

        # Write to KUDU table
        finalSensor.write.format('org.apache.kudu.spark.kudu')\
            .option("kudu.master", '10.0.0.48')\
            .option("kudu.table", "sensor_measurements")\
            .mode("append")\
            .save()
    except:
        pass

sensorDS.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
