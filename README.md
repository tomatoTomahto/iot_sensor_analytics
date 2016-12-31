# cdh_historian
## Pre-Requisites
1. CDH 5.10, including the following services:
* Impala
* Kafka
* KUDU
* Spark 2.0
* Solr
* HUE
2. StreamSets Data Collector 2.2.1.0
3. Anaconda Parcel for Cloudera
4. Anaconda Distribution of Python 2.7, including the following modules:
* kudu
* kafka

## Setup Instructions
1. Create a Kafka topic 
2. Edit config.ini with the desired data generator and hadoop settings
3. Create the required Impala tables
4. Create the required Solr collections

## Execution Instructions
1. Start the Spark streaming application
2. Start the data generator application
3. Start the StreamSets pipeline

## Analysis Instructions
1. StreamSets
2. HUE
