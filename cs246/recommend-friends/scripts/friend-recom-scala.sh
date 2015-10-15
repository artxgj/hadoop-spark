#!/bin/bash
namenode="hdfs://localhost:8020"
hdfs_output_path=/users/art/cs246/recommend-friends/output-scala
hdfs_input_url="${namenode}/users/art/cs246/recommend-friends/input"
hdfs_output_url="${namenode}${hdfs_output_path}"

hdfs dfs -rm -r ${hdfs_output_path}
master=yarn
#master=local[*]

${SPARK_HOME}/bin/spark-submit \
     --class org.learningisfun.spark.FriendRecom \
     --master $master \
     ../target/friendrecom-1.0-SNAPSHOT.jar ${hdfs_input_url} ${hdfs_output_url} 12
