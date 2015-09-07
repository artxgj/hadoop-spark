#!/usr/bin/env bash

hadoop fs -rm -r /user/art/cs246/output/recommended-friends-1

$SPARK_HOME/bin/spark-submit --master yarn-cluster recommend-friends-1.py --py-files $SPARK_HOME/python/lib/pyspark.zip
