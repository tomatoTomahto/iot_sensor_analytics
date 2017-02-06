#!/bin/bash
export PATH=/opt/cloudera/parcels/Anaconda/bin:$PATH
sudo yum install -y wget gcc-c++ curl
wget http://archive.cloudera.com/kudu/redhat/7/x86_64/kudu/cloudera-kudu.repo
sudo mv cloudera-kudu.repo /etc/yum.repos.d/
sudo yum install kudu-client0 kudu-client-devel -y
curl "https://bootstrap.pypa.io/get-pip.py" -o "get-pip.py"
sudo python get-pip.py
LDFLAGS=-L/opt/cloudera/parcels/Anaconda/lib pip install kudu-python
pip install kafka
pip install configparser
sudo mkdir /opt/sdclib
sudo mkdir /opt/sdclib/streamsets-datacollector-cdh_5_10-lib
sudo mkdir /opt/sdclib/streamsets-datacollector-cdh_5_10-lib/lib
sudo chmod -R a+rw /opt/sdclib/
sudo cp streamsets/*.jar /opt/sdclib/streamsets-datacollector-cdh_5_10-lib/lib/
