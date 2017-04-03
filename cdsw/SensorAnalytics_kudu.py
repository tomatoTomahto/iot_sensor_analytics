# # Sensor Analytics Demo
# ### Samir Gupta - April 4, 2017
# ## Overview
# ### The Business Challenge: How can we reduce our maintenance spend?
# Organizations spend millions per year on both planned and unplanned maintenance
# activities on their field assets. Studies show that on average, 1-in-6 maintenance 
# events are unplanned and over 60% of planned maintenance does not lead to improved
# asset performance. Companies are increasingly looking to their data to understand
# how efficient their maintenance processes actually are and if there are opportunies
# to reduce planned and unplanned asset outages using predictive analytics.
# ### The Technology Challenge: How can we analyze all our maintenance data?
# There are a variety of data sources that can be used to understand maintenance trends:
# * Text-based maintenance logs recorded each time an inspection or parts replacement occurs
# * Invoices from vendors indicating the type and cost of each maintenance activity
# * Images and videos of the assets in production
# * Sensor data from field assets, Scada systems, Historians, and other sources
# * External data sources including weather and traffic data
# Existing technologies have many challenges in ingesting, processing, and exposing
# this data. These data sources can be extremely large both in volume and variety,
# and current relational database systems (RDBMS) are not capable of scalably,
# performantly, and cost-effectively ingesting, processing, storing, and analyzing it.
# As a result, the business is not able to:
# * analyze un-structured sources like text, video, audio, and images
# * analyze granular (second-level) data across many years of history
# * easily ingest large volumes of data external to the organization
# * build accurate predictive models 
# There is also a requirement from the IT organization to secure this data using the
# latest authentication, encryption and access control frameworks. 
# ### The Solution
# More and more companies are looking to the latest big data technologies to solve these
# challenges. Hadoop-based solutions offer organizations the ability to store, process
# and analyze unlimited amounts of data using a scalable architecture, any kind of data 
# (structured or un-structured) using flexible storage formats, and perform large-scale
# visual and predictive analytics on that data using community-driven analytics frameworks.
# The Cloudera Enterprise Data Hub is a platform built on open-source technology that 
# provides all the benefits outlined above with the added capabilities of enterprise
# grade security, management, and data analytics tooling that organizations require. 
# ### The Demo
# This demo, built with Cloudera's Data Science Workbench, walks through the process
# of analyzing some of the data sources described above, and building a machine learning
# model to predict planned and unplanned maintenance outages. 
# The following data sources will be used:
# * Maintenance logs with engineer notes for every maintenance activity that was done on the asset
# * Sensor readings from every sensor on the asset
# ### The Frameworks
# #### Apache Spark
# Apache Spark is a fast and general engine for large-scale data processing that enables:
# * Fast Analytics - Spark runs programs up to 100x faster than Hadoop MapReduce in memory, or 10x faster on disk.
# * Easy Data Science - With APIs in Java, Scala, Python, and R, it's easy to build parallel apps.
# * General Processing - Spark's libraries enable SQL and DataFrames, machine learning, graph processing, and stream processing.

# #### Apache Impala
# Apache Impala is the open source, native analytic database for Apache Hadoop. Impala 
# is shipped by Cloudera, MapR, Oracle, and Amazon and enables:
# * BI-style Queries on Hadoop - low latency and high concurrency for BI/analytic queries on Hadoop
# * Enterprise-class Security - integrated with native Hadoop security and Kerberos for authentication, and Sentry for role-based access control.
# * Huge BI Tool Ecosystem - all leading BI, visualization and analytics tools integrate with Impala

# #### Apache Kudu
# Apache Kudu is a scalable storage engine capable of ingesting and updating real-time data while
# enabling large-scale machine learning and deep analytics on that data. It's capabilities include:
# * Streamlined Architecture - fast inserts/updates and efficient columnar scans to enable multiple real-time analytic workloads across a single storage layer. 
# * Faster Analytics - specifically designed for analytics on rapidly changing data. Kudu lowers query latency significantly for Apache Impala and Apache Spark 

# # Demo
# ## Setup Tasks
# * Install Python packages used by the demo
!conda install -y ConfigParser

# ## Initialization
# Spark Library Imports
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.types import DateType, StringType, FloatType, StructField, StructType, IntegerType
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, CountVectorizer
from pyspark.ml.clustering import LDA

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

# ### Create a Spark Session
spark = SparkSession.builder.appName("Sensor Analytics").getOrCreate()
sc = spark.sparkContext
sqc = SQLContext(sc)

# ## Analyze Maintenance Costs
# We start our analysis with visualizing the distribution of maintenance costs
# ### Histogram of Maintenance Costs
rawMaintCosts = sc.textFile("maintenance/maintenance_costs.csv").map(lambda l: l.split(","))    
schemaString = "date cost"
schema = StructType([StructField(field_name, StringType(), True) for field_name in ['date','cost']])
maintCosts = spark.createDataFrame(rawMaintCosts, schema)
maintCosts = maintCosts.select(maintCosts.date.cast('date'), maintCosts.cost.cast('int'))
maintCostsPD = maintCosts.toPandas()
maintCostsPD.describe()
sb.distplot(maintCostsPD['cost'], bins=100, hist=False, kde_kws={"shade": True}).set(xlim=(0, max(maintCostsPD['cost'])))

# ## Text Analytics of Maintenance Logs
# Spark provides rich text analytics capabilities including nGram extraction, TF-IDF, 
# stop words removal, vectorization, and more that can be used to build machine learning
# models based on textual data. 
# ### Sample of Maintenance Logs
maintenance = spark.read.format("com.databricks.spark.csv").option("delimiter", "|")\
  .load("maintenance/maintenance_logs.txt")\
  .withColumnRenamed('_c0','date')\
  .withColumnRenamed('_c1','note')\
  .withColumnRenamed('_c2','duration')\
  .withColumn('note', F.lower(F.regexp_replace('note', '[.!?-]', '')))\
  .select(F.col('date').cast('date'), 'note', F.col('duration').cast('int'))
maintenance.show(5, truncate=False)

# ### Sample of 3-word nGrams on Maintenance Notes
tk = Tokenizer(inputCol="note", outputCol="words") # Tokenize
maintTokenized = tk.transform(maintenance)
swr = StopWordsRemover(inputCol="words", outputCol="filtered") # Remove stop-words
maintFiltered = swr.transform(maintTokenized)
ngram = NGram(n=2, inputCol="filtered", outputCol="ngrams") # 3-word nGrams
maintNGrams = ngram.transform(maintFiltered)
maintNGrams.select('ngrams').show(5, truncate=False)

# ### Topic Clustering using Latent Dirichlet allocation (LDA)
# LDA is a form of un-supervised machine learning that identifies clusters, or topics,
# in the data
cv = CountVectorizer(inputCol="ngrams", outputCol="features", vocabSize=50)\
  .fit(maintNGrams) # CountVectorize converts nGram array into a vector of counts
maintVectors = cv.transform(maintNGrams)
vocabArray = cv.vocabulary
lda = LDA(k=3, maxIter=10)
ldaModel = lda.fit(maintVectors)
topics = ldaModel.describeTopics(5)
# We see below that each maintenance log can be clustered based on its text into 
# 1 of 3 topics below. The nGrams in each cluster show clearly 3 types of maintenance
# activities
# 1. Preventive maintenance occurs when the we have 'abnormal readings' or a 'component replacement'
# 2. Corrective maintenance occurs when we have a 'asset shutdown' event or 'asset failure'
# 3. The rest of the logs indicate that no downtime is required (ie. 'maintenance tests passed', 'asset healthy')
for topic in topics.collect():
    print('Topic %d Top 5 Weighted nGrams' % (topic[0]+1))
    for termIndex in topic[1]:
        print('  %s' % vocabArray[termIndex])

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
  .select('date',findCluster('topicDistribution').alias('maintenanceType'), 'duration')

maintClusters = maintTypes.join(maintCosts, 'date')\
  .select('maintenanceType', 'cost', 'duration')\
  .toPandas()

from scipy.stats import kendalltau
sb.set(style="ticks")
sb.jointplot(maintClusters['cost'], maintClusters['duration'], 
             kind="hex", stat_func=kendalltau, color="#4CB391")

sb.pairplot(maintClusters, hue="maintenanceType", vars=['cost','duration'])

# ## Visualization and Machine learning on Sensor Data
# Now that we are able to use the maintenance logs to identify the type of maintenance
# that occurs on each day (preventive, corrective, or program/routine), let's combine
# our maintenance data with sensor data to understand the correlations between the two. 
# ### Visualization of Sensor Data
tagIDs = sqc.read.format('org.apache.kudu.spark.kudu')\
	.option('kudu.master',kuduMaster)\
	.option('kudu.table','tag_mappings').load()
  
rawMeasurements = sqc.read.format('org.apache.kudu.spark.kudu')\
	.option('kudu.master',kuduMaster)\
	.option('kudu.table','raw_measurements').load()\
  .join(tagIDs, 'tag_id')\
  .select(F.col('record_time').cast('timestamp'), 'sensor_name', 'value', 'tag_id')
rawMeasurements.show()
rawMeasurements.cache()

dailyRawMeasurements = rawMeasurements.groupBy(F.to_date('record_time').alias('day'), 'sensor_name', 'tag_id')\
  .agg(F.avg('value').alias('value'))\
  .orderBy('day')
    
sb.set(style="ticks", palette="muted", color_codes=True)
ax = sb.boxplot(x="value", y="sensor_name", 
                data=dailyRawMeasurements.filter('value!=0 and year(day)>=2016').select('sensor_name','value').toPandas(),
                whis=np.inf, color="c")
sb.despine(trim=True)

sb.set(style="whitegrid", palette="muted")
maintSensorDailyPD = maintTypes.filter('year(date)>=2016')\
  .select(maintTypes.date.alias('day'), 'maintenanceType')\
  .join(dailyRawMeasurements, 'day')\
  .select('maintenanceType', 'sensor_name', 'value')\
  .toPandas()
sb.swarmplot(y='sensor_name', x='value', hue='maintenanceType', data=maintSensorDailyPD)

# Let's plot the maintenance type against sensor readings right before we have an outage
progPrevMaint = maintTypes.filter('maintenanceType!="Corrective"')\
  .select('date', 'maintenanceType')\
  .join(dailyRawMeasurements, maintTypes.date==dailyRawMeasurements.day)\
  .select('date', 'maintenanceType', 'tag_id', 'value', 'sensor_name')

corrMaint = maintTypes.filter('maintenanceType=="Corrective"')\
  .select('date', 'maintenanceType')\
  .join(dailyRawMeasurements, maintTypes.date==F.date_add(dailyRawMeasurements.day, 1))    .select('date', 'maintenanceType', 'tag_id', 'value', 'sensor_name')

rawSensorsByMaint = progPrevMaint.union(corrMaint)

sb.swarmplot(y='sensor_name', x='value', hue='maintenanceType', 
             data=rawSensorsByMaint.filter('year(date)>=2016').select('sensor_name', 'value', 'maintenanceType').toPandas())

# ### Spark Machine Learning Capabilities
# The analysis and visualizations above indicate that there is some relationship between
# sensor readings and the type of maintenance that occurs. Thus we should be able to
# build a model to predict this.
# Spark has an extensive list of built-in machine learning algorithms, including:
# * Classification algorithms
#  * Binomial and multi-nomial logistic regression
#  * Decision tree and random forest classifiers
#  * Gradient-boosted tree classifiers
#  * Multi-layer perception classifiers
#  * One-vs-Rest/All Classifier
#  * Naive Bayes
# * Regression algorithms
#  * Linear and Generalized Linear Regression
#  * Decision tree, random forest and gradient-boosted tree regression
#  * Survival regression
#  * Isotonic regression

# This demo will use a Random Forest Classifier to predict the type of maintenance that
# needs to occur based on sensor readings. Random Forests combine many decision trees 
# in order to reduce the risk of model overfitting. The spark.ml implementation 
# supports random forests for binary and multiclass classification and for regression, 
# using both continuous and categorical features.
# First let's import the Spark ML libraries
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors

# #### Data Preparation
# Before using any model, the data needs to be organized into a set of 'features' and 'labels'.
# In this case, a feature is a sensor reading, and the label is what maintenance type 
# that reading resulted in. Let's pivot the data so that we have a column for each sensor
sensorNames = rawSensorsByMaint.select('sensor_name').distinct().toPandas()\
  .values.flatten()
modelData = rawSensorsByMaint.groupBy('date','maintenanceType')\
  .pivot('sensor_name', sensorNames)\
  .agg(F.avg(F.round('value')))
modelData.persist()
modelData.select('date', 'Sensor_5').show()
modelData.select('Sensor_5', 'Sensor_10').describe().show()

# Now we need to convert our feature columns (sensor names) into a vector for each row
va = VectorAssembler(inputCols=sensorNames, outputCol="features")\
  .transform(modelData)
# Index the labels (maintenance type)
li = StringIndexer(inputCol='maintenanceType', outputCol='label')\
  .fit(va)

# #### Model Training
# We split the data into 2 subsets - one to train the model, and one to test/evaluate it 
# We then build a pipeline of all steps involved in running the model on some data. The
# pipeline will have the following steps:
# 1. StringIndexer - convert the maintenance type strings to a numeric index
# 2. RandomForestClassifier - classify the data into one of the different indexes
# 3. IndexToString - convert he maintenance type indexes back to strings
(trainingData, testData) = va.randomSplit([0.7, 0.3])
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
i2s = IndexToString(inputCol="prediction", outputCol="predictedLabel",
                    labels=li.labels)
pipeline = Pipeline(stages=[li, rf, i2s])
model = pipeline.fit(trainingData)

# #### Model Evaluation
# The training data was used to fit the model (ie. train it), now we can test the model
# using the test subset, and calculate the accuracy (ie. false prediction rate)
predictions = model.transform(testData)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

predictions.groupBy(predictions.predictedLabel.alias('Prediction'),
                    predictions.maintenanceType.alias('Actual'))\
    .count().toPandas()

# #### Feature Importances
# Spark also provides the ability to extract the relative importance of each feature
# that makes up the model. We see below that a subset of sensor readings do not have 
# any influence in predicting the type of maintenance
rfModel = model.stages[1]
fi = rfModel.featureImportances.toArray()

sensorImportances = {}
for sensorIndex in range(len(fi)):
    sensorImportances[sensorNames[sensorIndex]] = round(fi[sensorIndex]*100)
    
sensorImportancesPD = pd.DataFrame(sensorImportances.items(), columns=['Sensor','Importance (%)'])                           .sort_values('Importance (%)')
    
sb.set_color_codes("pastel")
sb.barplot(x="Importance (%)", y="Sensor", 
           data=sensorImportancesPD,
           label="Total", color="b")

# We can also graph the correlation matrix for the sensors - TODO
meas = rawMeasurements.filter(F.year('record_time')>2016)\
  .groupBy('record_time')\
  .pivot('sensor_name')\
  .agg(F.avg('value'))

sensorNameArray = meas.columns
sensorNameArray.remove('record_time')
corr = meas.select(sensorNameArray).toPandas().corr()
mask = np.zeros_like(corr, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

cmap = sb.diverging_palette(220, 10, as_cmap=True)
sb.heatmap(corr, mask=mask, xticklabels=sensorNameArray, yticklabels=sensorNameArray,
            square=True, linewidths=.5, cbar_kws={"shrink": .5})

# #### Model Tuning
# Spark has advanced model tuning capabilities as well. Let's improve our Random Forest
# Classifier using the ML tuning api
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit

# ParamGrids are grids of model tuning parameter values
paramGrid = ParamGridBuilder()\
  .addGrid(rf.maxDepth, [5,10,15])\
  .addGrid(rf.numTrees, [20,25,30])\
  .build()

# A TrainValidationSplit is used for hyper-parameter tuning. It takes a model estimator,
# parameter grid, and evaluator as input and runs the model multiple times to identify
# the most optimal model parameters
tvs = TrainValidationSplit(estimator=rf,
                           estimatorParamMaps=paramGrid,
                           evaluator=MulticlassClassificationEvaluator(),
                           trainRatio=0.8)

(trainingData, testData) = li.transform(va).randomSplit([0.7, 0.3])

# Run TrainValidationSplit, and choose the best set of parameters.
model = tvs.fit(trainingData)

predictions = model.transform(testData)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

i2s.transform(predictions).groupBy('predictedLabel', 'maintenanceType')\
    .count().toPandas()
  
fi = model.bestModel.featureImportances.toArray()

sensorImportances = {}
for sensorIndex in range(len(fi)):
    sensorImportances[sensorNames[sensorIndex]] = round(fi[sensorIndex]*100)
    
sensorImportancesPD = pd.DataFrame(sensorImportances.items(), columns=['Sensor','Importance (%)'])\
  .sort_values('Importance (%)')
    
sb.set_color_codes("pastel")
sb.barplot(x="Importance (%)", y="Sensor", 
           data=sensorImportancesPD,
           label="Total", color="b")

# #### Model Saving/Loading
# We can save models and pipelines for re-use later 
model.bestModel.write().overwrite().save(path='rf_sensor_maintenance.mdl')
!hdfs dfs -get rf_sensor_maintenance.mdl

newModel = RandomForestClassificationModel.load('rf_sensor_maintenance.mdl')
predictions = newModel.transform(testData)
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))

# That's it! We have successfully built a machine learning model that predicts with over
# 95% accuracy whether maintenance needs to be done on our asset using sensor data. 
