from IPython.display import Image

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
Image(filename="img/cloudera.png")
# The Cloudera Enterprise Data Hub is a platform built on open-source technology that 
# provides all the benefits outlined above with the added capabilities of enterprise
# grade security, management, and data analytics tooling that organizations require. 
# The architecture below illustrates how easy a use case like predictive maintenance is
# using the Cloudera technology stack. 
Image(filename="img/architecture.png")

# ### The Frameworks
# #### Apache Spark
Image(filename="img/spark.png")
# Apache Spark is a fast and general engine for large-scale data processing that enables:
# * Fast Analytics - Spark runs programs up to 100x faster than Hadoop MapReduce in memory, or 10x faster on disk.
# * Easy Data Science - With APIs in Java, Scala, Python, and R, it's easy to build parallel apps.
# * General Processing - Spark's libraries enable SQL and DataFrames, machine learning, graph processing, and stream processing.

# #### Apache Impala
Image(filename='img/impala.png')
# Apache Impala is the open source, native analytic database for Apache Hadoop. Impala 
# is shipped by Cloudera, MapR, Oracle, and Amazon and enables:
# * BI-style Queries on Hadoop - low latency and high concurrency for BI/analytic queries on Hadoop
# * Enterprise-class Security - integrated with native Hadoop security and Kerberos for authentication, and Sentry for role-based access control.
# * Huge BI Tool Ecosystem - all leading BI, visualization and analytics tools integrate with Impala

# #### Apache Kudu
Image(filename='img/kudu.png')
# Apache Kudu is a scalable storage engine capable of ingesting and updating real-time data while
# enabling large-scale machine learning and deep analytics on that data. It's capabilities include:
# * Streamlined Architecture - fast inserts/updates and efficient columnar scans to enable multiple real-time analytic workloads across a single storage layer. 
# * Faster Analytics - specifically designed for analytics on rapidly changing data. Kudu lowers query latency significantly for Apache Impala and Apache Spark 

# ### The Demo
# This demo, built with Cloudera's Data Science Workbench, walks through the process
# of analyzing some of the data sources described above, and building a machine learning
# model to predict planned and unplanned maintenance outages. 
# The following data sources will be used:
# * Maintenance data for every maintenance activity that was done on all assets
# * Sensor readings from every sensor on each asset

# # Demo Start
# ## Setup Tasks
# * Install Python packages used by the demo
# * Connect to Apache Spark
# * Connect to Apache Kudu
!pip install seaborn

# ## Initialization
# ### Spark Library Imports
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

# ### Other Python Library Imports
get_ipython().magic(u'matplotlib inline')
from IPython.display import display, HTML
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np
import pandas as pd
import ConfigParser

# ### Read in Kudu information
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
rawMaintCosts = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kuduMaster)\
    .option('kudu.table','impala::sensors.maintenance').load()\
  .withColumn('day', F.to_date(F.from_unixtime('maint_date')))\
  .withColumn('month', F.date_format(F.from_unixtime('maint_date'),'yyyy-MMM'))\
  .orderBy('maint_date')
maintCosts = rawMaintCosts.toPandas()

# ### Summary Statistics on Maintenance Costs
maintCosts.describe()

# ### Boxplot of Monthly Maintenance Costs
sb.set(style="ticks", palette="muted", color_codes=True)
sb.boxplot(x="cost", y="month", data=maintCosts, whis=np.inf, color='r')
sb.despine(trim=True)

# ### Pairplot Comparing Maintenance Cost and Duration
sb.pairplot(maintCosts, hue="type", vars=['cost','duration'])

# We see that there are 3 distinct types of maintenance activities that occur on our assets:
# 1. **Routine Maintenance** - low cost, low duration, general checkups. We want to eliminate these costs with predictive maintenance
# 2. **Preventative Maintenance** - medium cost, medium duration, fix issues before they occur. We want this to be the only type of maintenance that occurs. 
# 3. **Corrective Maintenance** - high cost, high duration, fix issues after they occur with downtime. We want to eliminate these costs with predictive maintenance. 

# ## Visualization and Machine learning on Sensor Data
# Let's combine our maintenance data with sensor data to understand the correlations between the two. 
# ### Read in Sensor Data from Kudu
sensors = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kuduMaster)\
    .option('kudu.table','impala::sensors.asset_sensors').load()
  
assets = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kuduMaster)\
    .option('kudu.table','impala::sensors.well_assets').load()
  
rawMeasurements = sqc.read.format('org.apache.kudu.spark.kudu')\
    .option('kudu.master',kuduMaster)\
    .option('kudu.table','impala::sensors.measurements').load()\
  .join(sensors, 'sensor_id')\
  .join(assets, 'asset_id')\
  .withColumn('record_time',F.from_unixtime('time'))\
  .withColumn('day',F.to_date('record_time'))\
  .join(rawMaintCosts, ['day','asset_id'], 'leftouter')\
  .withColumn('isMaintenance', F.col('type').isin('CORRECTIVE','PREVENTATIVE'))\
  .select('asset_name','sensor_name','value', 'isMaintenance', 'cost')
rawMeasurements.show()

# ### Spark Machine Learning Capabilities
# The analysis and visualizations above indicate that there is some relationship between
# sensor readings and the type of maintenance that occurs. Thus we should be able to
# build a model to predict this.
# Spark has an extensive list of built-in machine learning algorithms, we will use Random Forest Classification
Image(filename="img/randomforest.jpg")

# First let's import the Spark ML libraries
from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel
from pyspark.ml.feature import IndexToString, StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.linalg import Vectors

# #### Data Preparation
# Before using any model, the data needs to be organized into a set of 'features' and 'labels'.
# In this case, our features are sensor names and their readings, and the label is whether a
# particular asset needs maintenance or not. We'll use Spark's feature extraction libraries for this.
modelData = rawMeasurements.filter('isMaintenance')
si1 = StringIndexer(inputCol='sensor_name', outputCol='sensor_id').fit(modelData).transform(modelData)
va = VectorAssembler(inputCols=['sensor_id','value'], outputCol="features").transform(si1)
li = StringIndexer(inputCol='asset_name', outputCol='label').fit(va)

# #### Model Training
# We split the data into 2 subsets - one to train the model, and one to test/evaluate it 
(trainingData, testData) = va.randomSplit([0.7, 0.3])
rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
li2s = IndexToString(inputCol="prediction", outputCol="predictedLabel",labels=li.labels)
pipeline = Pipeline(stages=[li, rf, li2s])
model = pipeline.fit(trainingData)

# #### Model Evaluation
# The training data was used to fit the model (ie. train it), now we can test the model
# using the test subset, and calculate the accuracy (ie. false prediction rate)
predictions = model.transform(testData)
evaluator = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)

# Our model is very accurate, let's visualize the results. A heatmap can show how many correct and incorrect predctions we made
predictionResults = predictions.groupBy(predictions.predictedLabel.alias('Prediction'),
                    predictions.asset_name.alias('Actual'))\
    .count().toPandas()
predictionResults

# ## Heatmap - Predicted Maintenance vs. Actual Maintenance
sb.heatmap(predictionResults.pivot('Prediction', 'Actual', 'count'),cbar=False)

display(HTML("<font size=4><b>Predictive Maintenance Model Accuracy:</b> {0:.0f}%</font>".format(accuracy*100)))

# That's it! We have successfully built a machine learning model that predicts with over
# 80% accuracy when maintenance needs to be done on a particular asset using sensor data. 

