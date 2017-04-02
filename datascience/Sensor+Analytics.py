
# coding: utf-8

# In[197]:

from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, FloatType, StructField, StructType, IntegerType
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, CountVectorizer
from pyspark.ml.clustering import LDA

get_ipython().magic(u'matplotlib notebook')
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec # subplots
import pandas
from pandas.tools.plotting import table

spark = SparkSession     .builder     .appName("Sensor Analytics")     .getOrCreate()


# In[220]:

# Plot the maintenance costs
rawMaintCosts = sc.textFile("/Users/samir/Box Sync/Cloudera/Demos/cdh_historian/sampledata/maintenance_costs.csv")    .map(lambda l: l.split(","))
    
schemaString = "date cost"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)
maintCosts = spark.createDataFrame(rawMaintCosts, schema)
maintCosts = maintCosts.select(maintCosts.date.cast('date'), maintCosts.cost.cast('int'))


# In[264]:

maintenance = spark.read.format("com.databricks.spark.csv")    .option("delimiter", "|")    .load("/Users/samir/Box Sync/Cloudera/Demos/cdh_historian/sampledata/maintenance_logs.txt")    .withColumnRenamed('_c0','date')    .withColumnRenamed('_c1','note')    .withColumn('note', F.lower(F.regexp_replace('note', '[.!?-]', '')))    .select(F.col('date').cast('date'), 'note')
maintenance.show(30, truncate=False)

# Tokenize maintenance notes
tk = Tokenizer(inputCol="note", outputCol="words")
maintTokenized = tk.transform(maintenance)

# Remove stop-words
swr = StopWordsRemover(inputCol="words", outputCol="filtered")
maintFiltered = swr.transform(maintTokenized)

# Compute 2-word n-Grams
ngram = NGram(n=3, inputCol="filtered", outputCol="ngrams")
maintNGrams = ngram.transform(maintFiltered)

# Compute n-Gram counts and vectorize
cv = CountVectorizer(inputCol="ngrams", outputCol="features", vocabSize=50).fit(maintNGrams)
maintVectors = cv.transform(maintNGrams)
vocabArray = cv.vocabulary

# Topic identification using LDA
lda = LDA(k=3, maxIter=10)
ldaModel = lda.fit(maintVectors)
topics = ldaModel.describeTopics(5)

for topic in topics.collect():
    print('Topic %d Top 5 Weighted nGrams' % (topic[0]+1))
    for termIndex in topic[1]:
        print('\t%s' % vocabArray[termIndex])

from pyspark.sql.functions import udf
def f(clusterDistribution):
    if clusterDistribution[0] > 0.5:
        return 1
    elif clusterDistribution[1] > 0.5:
        return 2
    else: return 3
    
findCluster = udf(f, IntegerType())
maint_types = ldaModel.transform(maintVectors).select('date','topicDistribution')    .withColumn('type',findCluster('topicDistribution'))    .select('date','type')
maint_types.show()


# In[223]:

monthly_maintenance_types = maint_types.withColumn('program', maint_types.type==1)    .withColumn('preventive', maint_types.type==2)    .withColumn('corrective', maint_types.type==3)    .groupBy(F.date_format('date','yyyyMM').alias('month'))    .agg(F.count(F.when(F.col('program'), 1)).alias('program'),
         F.count(F.when(F.col('preventive'), 1)).alias('preventive'),
         F.count(F.when(F.col('corrective'), 1)).alias('corrective'))\
    .orderBy('month')
    
monthly_maintenance_costs = maintCosts.groupBy(F.date_format('date','yyyyMM').alias('month'))    .agg(F.sum('cost').alias('cost'))    .orderBy('month')

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()
monthly_maintenance_types.toPandas().plot(kind='bar', stacked=True, x='month', ax=ax1,
                                         title='Maintenance Costs Per Month Overlaid With Maintenance Type', legend=True)
monthly_maintenance_costs.toPandas().plot(kind='line', x='month', y='cost', ax=ax2, color='DarkRed')


# In[194]:

maintVectors.filter(F.instr('note','corrective')>0)    .select(F.explode('ngrams').alias('ngram'))    .filter(F.instr('ngram','sensor')>0)    .groupBy('ngram')    .count()    .orderBy('count')    .toPandas().plot(kind='barh', x='ngram', title='Top nGrams About Sensors in Corrective Maintenance Logs', legend=False)
plt.tight_layout()


# In[269]:

# Read in Sensor Measurements and Maintenance Notes
measurements = spark.read.json("/Users/samir/Box Sync/Cloudera/Demos/cdh_historian/sampledata/measurements.json")
# measurements.describe().show()

fig, axes = plt.subplots(2,1)
# measurements.groupBy(F.date_format('record_time', 'yyyyMM').alias('month'))\
#     .agg((F.avg('flow_rate')/30).alias('flow_rate/30'),
#          F.avg('liquid_level').alias('liquid_level'), 
#          F.avg('pipe_pressure').alias('pipe_pressure'), 
#          F.avg('temperature').alias('temperature'), 
#          F.avg('torque').alias('torque'))\
#     .orderBy('month')\
#     .toPandas()\
#     .plot(kind='line', x='month', title='Average Monthly Sensor Levels (2016-2017)')

a = measurements.filter(F.add_months('record_time',3)>=F.current_date())    .groupBy(F.date_format('record_time', 'yyyyMMdd').alias('day'))    .agg((F.avg('flow_rate')/30).alias('flow_rate/30'),
         F.avg('liquid_level').alias('liquid_level'), 
         F.avg('pipe_pressure').alias('pipe_pressure'), 
         F.avg('temperature').alias('temperature'), 
         F.avg('torque').alias('torque'))\
    .orderBy('day')\
    .toPandas()
a.plot(kind='line', x='day', title='Average Daily Sensor Levels (Jan-Mar 2017)', ax=axes[0], legend=False)
axes[0].set_xticks(range(len(a)))
axes[0].set_xticklabels(["%s" % item for item in a.day.tolist()], rotation=90)


b = maint_types.filter(F.add_months('date',3)>=F.current_date())    .select(F.date_format('date','yyyyMMdd').alias('day'), 'type')    .orderBy('day')    .toPandas()
b.plot(kind='line', x='day', y='type', style='o', ax=axes[1], title='Type of Maintenance Performed')
    
axes[1].set_xticks(range(len(b)))
axes[1].set_xticklabels(["%s" % item for item in b.day.tolist()], rotation=90)
    
plt.tight_layout()


# In[67]:

fig, axes = plt.subplots()
#ax4 = ax3.twinx()

# Read in Sensor Measurements and Maintenance Notes
measurementsPD = measurements.filter(F.year('record_time')>=2016)    .groupBy(F.date_format('record_time', 'yyyyMMdd').alias('day'))    .agg((F.avg('flow_rate')/30).alias('scaled flow_rate'),
         F.avg('liquid_level').alias('liquid_level'), 
         F.avg('pipe_pressure').alias('pipe_pressure'), 
         F.avg('temperature').alias('temperature'), 
         F.avg('torque').alias('torque'))\
    .orderBy('day')\
    .toPandas()

measurementsPD.plot(kind='line', x='day', title='Average Daily Sensor Levels (2016-2017)', ax=axes)
plt.tight_layout()
# measurements.filter(F.year('record_time')>=2016)\
#     .groupBy(F.date_format('record_time', 'yyyyMMdd').alias('day'))\
#     .agg((F.avg('flow_rate')).alias('flow_rate'))
#     .orderBy('day')\
#     .toPandas()\
#     .plot(kind='line', x='day', title='Average Daily Flow Rate Overlaid With Maintenance Type')

# monthly_maintenance_types = maint_types.filter(F.year('date')>=2016)\
#     .withColumn('program', maint_types.type==2)\
#     .withColumn('preventive', maint_types.type==1)\
#     .withColumn('corrective', maint_types.type==3)\
#     .groupBy(F.date_format('date','yyyyMMdd').alias('day'))\
#     .agg(F.count(F.when(F.col('program'), 1)).alias('program'),
#          F.count(F.when(F.col('preventive'), 1)).alias('preventive'),
#          F.count(F.when(F.col('corrective'), 1)).alias('corrective'))\
#     .orderBy('day')\
#     .toPandas()\
#     .plot(kind='scatter', x='day', y='program', color='green', ax=ax4)


# In[ ]:



