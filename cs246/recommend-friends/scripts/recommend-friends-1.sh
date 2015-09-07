#!/usr/bin/env bash

namenode="hdfs://localhost:8020"
hdfs_output_path=/user/art/cs246/output/recommended-friends-1-py
hdfs_input_url="${namenode}/user/art/cs246/input/friendslist"
hdfs_output_url="${namenode}${hdfs_output_path}"

hadoop fs -rm -r ${hdfs_output_path}

$SPARK_HOME/bin/spark-submit \
--master yarn-cluster \
../src/main/python/recommend-friends-1.py \
-i ${hdfs_input_url} \
-o ${hdfs_output_url} \
-p 16
