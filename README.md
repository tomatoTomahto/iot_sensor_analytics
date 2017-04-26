# Predictive Maintenance Using Sensor Analytics on Cloudera
For information on use case and technical architecture, see PDF in ```slides``` directory. 

## Pre-Requisites
1. CDH 5.10, including the following services:
 * Impala
 * Kafka (optional - for real-time ingest part of demo)
 * KUDU (optional - if not using sample data provided)
 * Spark 2.0
 * Solr (optional)
 * HUE
2. Cloudera Data Science Workbench
3. Anaconda Parcel for Cloudera
4. Streamsets Data Collector (optional)

## Generate Data (optional - if re-generating sample data or storing data in Kudu)
NOTE: You may need to install the kudu and kafka client libraries before running the data generation script:
For Kudu, install the client SDK then install the Python library. For Kafka, you just need to install the Python library. 
```
export PATH=/opt/cloudera/parcels/Anaconda/bin:$PATH
sudo yum install -y wget gcc-c++ curl
wget http://archive.cloudera.com/kudu/redhat/7/x86_64/kudu/cloudera-kudu.repo
sudo mv cloudera-kudu.repo /etc/yum.repos.d/
sudo yum install kudu-client0 kudu-client-devel -y
LDFLAGS=-L/opt/cloudera/parcels/Anaconda/lib pip install kudu-python
pip install kudu
pip install kafka
pip install configparser
```

1. Edit config.ini with the desired data generator parameters (# sensors etc.) and hadoop settings (kafka and kudu servers)
2. Create tables to store sensor data in Kudu and generate static lookup information:
```python datagen/historian.py config.ini static kudu```
3. Generate historic data and store in Kudu
```python datagen/historian.py config.ini historic kudu```
4. Open Kudu web UI and navigate to the tables that were created, extract Impala DDL statements and run them in HUE

## Data Science
1. In the Cloudera Data Science Workbench, create a new project using this git repository
2. Add spark-defaults.conf to the project environment settings
3. Create a workbench with at least 2 cores and 8GB RAM using PySpark
4. Run the cdsw/SensorAnalytics_kudu.py script

## Real-time Ingest (optional)
1. Start generating real-time data using the data generator
```python datagen/historian.py config.ini realtime```
2. In the Cloudera Data Science Workbench, run the cdsw/SensorAnalytics_stream.py script
3. (Optional) Create a pipeline in Streamsets to read from Kafka, transform data, and write to Kudu or HDFS 

## Analyze
1. Solr (not included in git repo)
 * Create a collection for the measurements data
 * Create a Hue dashboard based on the measurements data
 * Add a destination in Streamsets to send measurement data to Solr collection
2. Impala - open HUE and run queries on the raw_measurements and measurements tables
