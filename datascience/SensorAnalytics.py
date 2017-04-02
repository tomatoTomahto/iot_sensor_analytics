
## Copy over some data
!hdfs dfs -rm -r sampledata
!hdfs dfs -mkdir sampledata
!hdfs dfs -put sampledata/* sampledata/

## Spark Library Imports
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType, FloatType, StructField, StructType, IntegerType
from pyspark.sql import functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, NGram, CountVectorizer
from pyspark.ml.clustering import LDA

## Plotting Library Imports
get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt
import seaborn as sb
import numpy as np
import pandas as pd

## Create a Spark Session
spark = SparkSession.builder.appName("Sensor Analytics").getOrCreate()
sc = spark.sparkContext

# Step 1: Analyze Maintenance Costs and Logs
## Visualize Maintenance Costs and Plot Over Time
rawMaintCosts = sc.textFile("sampledata/maintenance_costs.csv").map(lambda l: l.split(","))    
schemaString = "date cost"
schema = StructType([StructField(field_name, StringType(), True) for field_name in ['date','cost']])
maintCosts = spark.createDataFrame(rawMaintCosts, schema)
maintCosts = maintCosts.select(maintCosts.date.cast('date'), maintCosts.cost.cast('int'))
maintCostsPD = maintCosts.toPandas()
maintCostsPD.head(20)
maintCostsPD.describe()
sb.distplot(maintCostsPD['cost'], bins=100, hist=False, kde_kws={"shade": True}).set(xlim=(0, max(maintCostsPD['cost'])))

## Analyze Maintenance Logs
maintenance = spark.read.format("com.databricks.spark.csv").option("delimiter", "|")\
  .load("sampledata/maintenance_logs.txt")\
  .withColumnRenamed('_c0','date')\
  .withColumnRenamed('_c1','note')\
  .withColumnRenamed('_c2','duration')\
  .withColumn('note', F.lower(F.regexp_replace('note', '[.!?-]', '')))\
  .select(F.col('date').cast('date'), 'note', F.col('duration').cast('int'))
  
maintenance.show(5, truncate=False)

### Text Analytics on Maintenance Notes
#### Tokenize
tk = Tokenizer(inputCol="note", outputCol="words")
maintTokenized = tk.transform(maintenance)

#### Remove stop-words
swr = StopWordsRemover(inputCol="words", outputCol="filtered")
maintFiltered = swr.transform(maintTokenized)

#### 3-word nGrams
ngram = NGram(n=3, inputCol="filtered", outputCol="ngrams")
maintNGrams = ngram.transform(maintFiltered)

#### CountVectorize converts nGram array into a vector of counts
cv = CountVectorizer(inputCol="ngrams", outputCol="features", vocabSize=50)\
  .fit(maintNGrams)
maintVectors = cv.transform(maintNGrams)
vocabArray = cv.vocabulary

#### Topic identification using Latent Dirichlet allocation (LDA)
lda = LDA(k=3, maxIter=10)
ldaModel = lda.fit(maintVectors)
topics = ldaModel.describeTopics(5)

#### Visualize Maintenance Notes by Topic
def f(clusterDistribution):
    if clusterDistribution[0] > 0.5:
        return 1
    elif clusterDistribution[1] > 0.5:
        return 2
    else: return 3
    
findCluster = F.udf(f, IntegerType())
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

for topic in topics.collect():
    print('Topic %d Top 5 Weighted nGrams' % (topic[0]+1))
    for termIndex in topic[1]:
        print('\t%s' % vocabArray[termIndex])

# Read in Sensor Measurements and Maintenance Notes
measurements = spark.read.json("measurements.json")

# Plot sensor measurements overlaid with maintenance types
monthlySensors = measurements.groupBy(F.date_format('record_time', 'yyyyMMdd').alias('month'))\
  .agg((F.avg('flow_rate')/30).alias('flow_rate (scaled)'),
        F.avg('liquid_level').alias('liquid_level'), 
        F.avg('pipe_pressure').alias('pipe_pressure'), 
        F.avg('temperature').alias('temperature'), 
        F.avg('torque').alias('torque'))\
  .orderBy('month')\
  .toPandas()

sns.set(style="ticks", palette="muted", color_codes=True)

# Load the example planets dataset
planets = sns.load_dataset("planets")

# Plot the orbital period with horizontal boxes
ax = sns.boxplot(x="distance", y="method", data=measurements,
                 whis=np.inf, color="c")

# Add in points to show each observation
sns.stripplot(x="distance", y="method", data=planets,
              jitter=True, size=3, color=".3", linewidth=0)


# Make the quantitative axis logarithmic
ax.set_xscale("log")
sns.despine(trim=True)
sb.boxplot(x="month", data=monthlySensors)

# Plot correlation of values
from pyspark.mllib.stat import Statistics

meas = measurements.select('flow_rate', 'liquid_level', 'pipe_pressure', 'temperature', 'torque')
features = meas.rdd.map(lambda row: row[0:])
corr_mat=Statistics.corr(features, method="pearson")
print(corr_mat)
# Generate a mask for the upper triangle
mask = np.zeros_like(corr_mat, dtype=np.bool)
mask[np.triu_indices_from(mask)] = True

# Generate a custom diverging colormap
cmap = sb.diverging_palette(220, 10, as_cmap=True)

sb.heatmap(corr_mat, mask=mask, xticklabels=meas.columns, yticklabels=meas.columns)

# Draw the heatmap with the mask and correct aspect ratio
sb.heatmap(corr_mat, mask=mask, cmap=cmap, vmax=.3,
           square=True, xticklabels=5, yticklabels=5,
           linewidths=.5, cbar_kws={"shrink": .5})
  
sb.heatmap(monthlySensors, linewidths=.5)

measurements.groupBy(F.date_format('record_time', 'yyyyMM').alias('month'))\
  .agg((F.avg('flow_rate')/30).alias('flow_rate/30'),
          F.avg('liquid_level').alias('liquid_level'), 
          F.avg('pipe_pressure').alias('pipe_pressure'), 
          F.avg('temperature').alias('temperature'), 
          F.avg('torque').alias('torque'))\
     .orderBy('month')\
     .toPandas()\
     .plot(kind='line', x='month', title='Average Monthly Sensor Levels (2016-2017)')

measurements.filter(F.add_months('record_time',3)>=F.current_date())\
  .groupBy(F.date_format('record_time', 'yyyyMMdd').alias('day'))\
  .agg((F.avg('flow_rate')/30).alias('flow_rate/30'),
        F.avg('liquid_level').alias('liquid_level'), 
        F.avg('pipe_pressure').alias('pipe_pressure'), 
        F.avg('temperature').alias('temperature'), 
        F.avg('torque').alias('torque'))\
    .orderBy('day')\
    .toPandas()\
    .plot(kind='line', x='day', title='Average Daily Sensor Levels (Jan-Mar 2017)')
    
axes[1].set_xticks(range(len(a)))
axes[1].set_xticklabels(["%s" % item for item in a.day.tolist()], rotation=90)


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




