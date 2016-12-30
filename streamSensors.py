from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession, Row, SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession\
            .builder\
            .config(conf=sparkConf)\
            .getOrCreate()
    return globals()['sparkSessionSingletonInstance']

spark = SparkSession\
    .builder\
    .appName("Streaming Historian Data")\
    .getOrCreate()

sc = spark.sparkContext
ssc = StreamingContext(sc, 10)
sqc = SQLContext(sc, spark)

sc.setLogLevel('FATAL')

lines = ssc.socketTextStream('10.0.0.57', 9999)
sensorDS = lines.map(lambda line: line.split(","))\
    .map(lambda (tag_id, value): (int(tag_id), int(value)))\
    .window(20,10)

fields = [StructField('tag_id', IntegerType()), StructField('value',IntegerType())]
schema = StructType(fields)

tag_mappings = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master','10.0.0.48')\
    .option('kudu.table','tag_mappings')\
    .load()

tag_mappings.persist()
tag_mappings.registerTempTable('tag_mappings')
tag_mappings.collect()
tag_mappings.show()

def process(time, rdd):
    print("========= %s =========" % str(time))
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())

        sensorDF = spark.createDataFrame(rdd, schema)
        sensorDF.registerTempTable('sensor_data')
#        sensorDF.show()

        tempSensor = sensorDF.join(tag_mappings, 'tag_id', 'inner')
        tempSensor.registerTempTable('df1')
#        tempSensor.show()

        fullSensor = tempSensor.join(tag_mappings, ['tag_id','tag_entity'], 'right_outer')\
            .filter(
            .select(tag_mappings.tag_entity, tag_mappings.tag_description, tempSensor.value)

#        fullSensor = sqc.sql('select tm.tag_id, tm.tag_entity, tm.tag_description, df1.value\
#                              from df1 right join tag_mappings tm\
#                                on (df1.tag_id == tm.tag_id and df1.tag_entity = tm.tag_entity)')
#        fullSensor.show()

        finalSensor = fullSensor.selectgroupBy('tag_entity')\
            .pivot('tag_description')\
            .agg(sum('value'))
        finalSensor.show()

    except:
        pass

sensorDS.foreachRDD(process)

ssc.start()
ssc.awaitTermination()
