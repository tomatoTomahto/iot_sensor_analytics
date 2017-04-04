# ## Initialization
# Spark Library Imports
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import DateType, StringType, FloatType, StructField, StructType, IntegerType
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, CountVectorizer
from pyspark.ml.clustering import LDA, LocalLDAModel

# Other Python Library Imports
get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np
import pandas as pd
import ConfigParser

# Read in Kudu information
config = ConfigParser.ConfigParser()
config.read('config.ini')
kuduMaster = config.get('hadoop','kudu_masters')
kuduPort = config.get('hadoop','kudu_port')

# Create a Spark Session
spark = SparkSession.builder.appName("Sensor Analytics").getOrCreate()
sc = spark.sparkContext
sqc = SQLContext(sc)

# ## Read in last 30 days worth of data
# Maintenance Logs
maintenance = spark.read.format("com.databricks.spark.csv").option("delimiter", "|")\
  .load("maintenance/maintenance_logs.txt")\
  .withColumnRenamed('_c0','date')\
  .withColumnRenamed('_c1','note')\
  .withColumnRenamed('_c2','duration')\
  .withColumn('note', F.lower(F.regexp_replace('note', '[.!?-]', '')))\
  .select(F.col('date').cast('date'), 'note', F.col('duration').cast('int'))\
  .filter(F.datediff(F.current_timestamp(), F.col('date').cast('date'))<30)

# Sensor Data
tagIDs = sqc.read.format('org.apache.kudu.spark.kudu')\
	.option('kudu.master',kuduMaster)\
	.option('kudu.table','tag_mappings').load()
rawMeasurements = sqc.read.format('org.apache.kudu.spark.kudu')\
	.option('kudu.master',kuduMaster)\
	.option('kudu.table','raw_measurements').load()\
  .filter(F.datediff(F.current_timestamp(), F.col('record_time').cast('timestamp'))<30)\
  .join(tagIDs, 'tag_id')\
  .select(F.col('record_time').cast('timestamp'), 'sensor_name', 'value', 'tag_id')
rawMeasurements.persist()

# ## Model Prediction
# Identify maintenance types using saved LDA model
tk = Tokenizer(inputCol="note", outputCol="words") # Tokenize
maintTokenized = tk.transform(maintenance)
swr = StopWordsRemover(inputCol="words", outputCol="filtered") # Remove stop-words
maintFiltered = swr.transform(maintTokenized)
ngram = NGram(n=2, inputCol="filtered", outputCol="ngrams") # 2-word nGrams
maintNGrams = ngram.transform(maintFiltered)
maintNGrams.select('ngrams').show(5, truncate=False)
cv = CountVectorizer(inputCol="ngrams", outputCol="features", vocabSize=50)\
  .fit(maintNGrams) # CountVectorize converts nGram array into a vector of counts
maintVectors = cv.transform(maintNGrams)
vocabArray = cv.vocabulary
ldaModel = LocalLDAModel.load('lda.mdl')

topic_1 = 'Corrective'
topic_2 = 'Preventive'
topic_3 = 'Healthy'
      
# ### Apply Maintenance Clusters (Types) to Maintenance Costs
# Let's use the clusters identified above to classify our maintenance costs
# This graph shows the distribution of cost vs. duration, color-coded by maintenance type
def f(clusterDistribution):
    if clusterDistribution[0] > 0.6:
        return topic_1
    elif clusterDistribution[1] > 0.6:
        return topic_2
    elif clusterDistribution[2] > 0.6: 
        return topic_3
    else: return 'Unknown'
    
findCluster = F.udf(f, StringType())
maintTypes = ldaModel.transform(maintVectors)\
  .select('date',findCluster('topicDistribution').alias('maintenanceType'))

# ## Data Transformations/Aggregations
sensorNames = rawMeasurements.select('sensor_name').distinct().toPandas()\
  .values.flatten()
  
dailyMeasurements = rawMeasurements.groupBy(F.to_date('record_time').alias('date'), 'sensor_name', 'tag_id')\
  .agg(F.avg('value').alias('value'))\
  .join(maintenance, 'date')\
  .groupBy('date','maintenanceType')\
  .pivot('sensor_name', sensorNames)\
  .agg(F.avg(F.round('value')))\
  .orderBy('date')
    
sb.set(style="ticks", palette="muted", color_codes=True)
ax = sb.boxplot(x="value", y="sensor_name", 
                data=dailyRawMeasurements.filter('value!=0 and year(day)>=2016').select('sensor_name','value').toPandas(),
                whis=np.inf, color="c")
sb.despine(trim=True)

# #### Model Saving/Loading
# We can save models and pipelines for re-use later 
model.bestModel.write().overwrite().save(path='rf_sensor_maintenance.mdl')
!rm -rf rf_sensor_maintenance.mdl
!hdfs dfs -get rf_sensor_maintenance.mdl

newModel = RandomForestClassificationModel.load('rf_sensor_maintenance.mdl')
predictions = newModel.transform(li.transform(va))
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))