#!/bin/bash
hdfs dfs -rm -r /user/art/cs246/output/recommended-friends-scala
master=yarn
#master=local[*]

${SPARK_HOME}/bin/spark-submit \
     --class org.learningisfun.spark.FriendRecom \
     --master $master \
     ../target/friendrecom-1.0-SNAPSHOT.jar
