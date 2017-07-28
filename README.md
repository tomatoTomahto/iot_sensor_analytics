# Predictive Maintenance Using Sensor Analytics on Cloudera
For information on use case and technical architecture, see PDF in ```slides``` directory. 

## Pre-Requisites
1. CDH 5.10+, including the following services:
 * Impala
 * Kafka (optional - for real-time ingest part of demo)
 * Kudu
 * Spark 2.0
 * HUE
2. Cloudera Data Science Workbench
3. Anaconda Parcel for Cloudera
4. Streamsets Data Collector (optional - for real-time ingest part of demo)

## Generate Historic Sensor Data
1. In HUE, open the Impala Query editor and run the DDL script int he ```impala``` directory. This will create the following tables:
 * Wells - well information like depth and geo-location
 * Well_Assets - asset information (ie. name)
 * Asset_Groups - asset categories that each asset will fall into
 * Asset_Sensors - sensor information (ie. name and asset that sensor belongs to)
 * Measurements - time-series sensor readings for each sensor in each well
 * Maintenance - day-series maintenance log whenever a maintenance event occurs
2. In the Cloudera Data Science Workbench, create a new project using this git repository
3. Create a workbench with at least 2 cores and 8GB RAM using PySpark
4. In the workbench, install the Kafka Python library: ```!pip install kafka```
5. Adjust the configuration settings in ```config.ini```, such as:
 * kudu masters
 * days of history to generate sensor readings for
 * interval between each measurement (default 1hr)
6. Open ```SensorData.py``` and run it

## Data Science Demo
Once all the data has been generated, open the ```cdsw/PredictiveMaintenance.py``` script and run it. 

## Real-time Sensor Data Ingest (optional - requires Kafka and Streamsets on the cluster)
1. Create a workbench in CDSW
2. Open ```config.ini``` and adjust the kafka settings - broker, topic and frequency of data (ie. 5 seconds)
3. Open ```SensorData.py``` and comment out the lines for generating static & historic data, and uncomment the lines for generating real-time data
 * Note: by default, this script will write data to Kafka. If you set kafka=False in the generateSensorData() function, data will be written straight to Kudu.
4. Run ```SensorData.py```

If data is written to Kafka, you have 2 options for real-time stream processing

### Option 1: Spark Streaming Applications
1. Open a new workbench in CDSW, and run the ```cdsw/StreamingAnalytics.py``` script.
This scripts creates a Spark Streaming job that reads in new data from Kafka, performs some transformations (enriches the data with well and asset info), displays it, and then rights the original data to Kudu. 

### Option 2: Streamsets Application
The Streamsets pipeline simply takes raw data from Kafka and writes it into the Kudu ```measurements``` table. 
The pipeline also performs a static lookup to enrich the measurements with a well_id and a sensor name, pivots the data so there is 1 column per sensor, and writes the resulting data to the ```pivot_measurement``` Kudu table. 
These static lookups could be replaced with JDBC lookups to the well and sensor metadata stored in Impala tables. 
There is an Impala view that dynamically aggregates the pivot_measurements table into 15 minute intervals and averages the readings. 

Steps to run Streamsets pipeline below:
1. Download the ```streamsets/Sensor Streaming [Kafka--Kudu].json``` file onto your laptop.
2. Open Streamsets and create a new pipeline using that file
3. Make sure that:
 * Streamsets user sdc is allowed to submit yarn jobs
 * Streamsets user sdc has a /user directory in HDFS with permissions
 * Spark 1.6 and Yarn gateway services are running on the Streamsets node
 * The pipeline mode is set to Cluster Yarn Streaming
 * The Kafka Consumer stage in the pipeline has the right zookeeper and kafka broker IPs
 * The Kudu stages in the pipeline have the right Kudu master 
4. Once the configuration settings have been made, run the pipeline. You should see data going across the stages. 
5. In HUE, query the ```pivot_measurements``` table or ```15m_agg_pivot_measurements``` view. 