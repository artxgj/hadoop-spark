#!/usr/bin/env bash
namenode="hdfs://localhost:8020"
hdfs_output_path=/user/art/cs246/recommend-friends/scala/output-2019
hdfs_input_url="${namenode}/user/art/cs246/recommend-friends/input"
hdfs_output_url="${namenode}${hdfs_output_path}"

hdfs dfs -rm -r ${hdfs_output_path}
master=yarn
#master=local[*]

${SPARK_HOME}/bin/spark-submit \
     --class org.learningisfun.spark.FriendRecomV2019 \
     --master $master \
     ../target/friendrecom-1.1-SNAPSHOT.jar ${hdfs_input_url} ${hdfs_output_url} 10 3
