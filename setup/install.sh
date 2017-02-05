#!/bin/bash
export PATH=/opt/cloudera/parcels/Anaconda/bin:$PATH
yum install -y wget gcc-c++
wget http://archive.cloudera.com/kudu/redhat/7/x86_64/kudu/cloudera-kudu.repo
mv cloudera-kudu.repo /etc/yum.repos.d/
yum install kudu-client0 kudu-client-devel -y
LDFLAGS=-L/opt/cloudera/parcels/Anaconda/lib pip install kudu-python
pip install kafka
pip install configparser
mkdir /opt/sdclib
mkdir /opt/sdclib/streamsets-datacollector-cdh_5_10-lib
mkdir /opt/sdclib/streamsets-datacollector-cdh_5_10-lib/lib
chmod -R a+rw /opt/sdclib/
cp streamsets/*.jar /opt/sdclib/streamsets-datacollector-cdh_5_10-lib/lib/
