#!/bin/bash

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

/etc/init.d/ssh start

if [ "$SPARK_WORKLOAD" == "master" ];
then

  hdfs namenode -format
  hdfs --daemon start namenode
  hdfs --daemon start secondarynamenode
  yarn --daemon start resourcemanager

  while ! hdfs dfs -mkdir -p /data-lake-logs;
  do
    echo "Fail to create data-lake-logs/ on HDFS"
  done
  
  echo "Success to create data-lake-logs/ on HDFS"
  hdfs dfs -mkdir -p /opt/spark/data
  echo "Folder /opt/spark/data created on HDFS"

  hdfs dfs -copyFromLocal /opt/spark/data/* /opt/spark/data
  hdfs dfs -ls /opt/spark/data

elif [ "$SPARK_WORKLOAD" == "worker" ];
then

  hdfs --daemon start datanode
  yarn --daemon start nodemanager
  
fi

tail -f /dev/null
