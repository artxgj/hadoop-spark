#!/usr/bin/env bash

hadoop fs -rm -r /user/art/cs246/output/recommended-friends-2
hdfs dfs -rm -r /user/art/tmp/friendrecom2
$SPARK_HOME/bin/spark-submit --master yarn-cluster recommend-friends-2.py --py-files $SPARK_HOME/python/lib/pyspark.zip
