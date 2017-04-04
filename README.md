# Sensor Analytics on Cloudera
For information on use case and technical architecture, see PDF in ```slides``` directory. 

## Pre-Requisites
1. CDH 5.10, including the following services:
 * Impala
 * Kafka
 * KUDU
 * Spark 2.0
 * Solr (Optional)
 * HUE
2. Cloudera Data Science Workbench
3. Anaconda Parcel for Cloudera

## Initialize
1. Edit config.ini with the desired data generator parameters (# sensors etc.) and hadoop settings (kafka and kudu servers)
2. Create tables to store sensor data in Kudu and generate static lookup information:
```python datagen/historian.py config.ini static```
3. Generate historic data and store in Kudu
```python datagen/historian.py config.ini historic```
4. Open Kudu web UI and navigate to the tables that were created, extract Impala DDL statements and run them in HUE

## Data Science
1. In the Cloudera Data Science Workbench, create a new project using this git repository
2. Create a workbench with at least 2 cores and 8GB RAM using PySpark
3. Run the cdsw/SensorAnalytics_kudu.py script

## Real-time Ingest
1. Start generating real-time data using the data generator
```python datagen/historian.py config.ini realtime```
2. In the Cloudera Data Science Workbench, run the cdsw/SensorAnalytics_stream.py script

## Analyze
1. Solr (not included in git repo)
 * Create a collection for the measurements data
 * Create a Hue dashboard based on the measurements data
 * Add a destination in Streamsets to send measurement data to Solr collection
2. Impala - open HUE and run queries on the raw_measurements and measurements tables
