#!/bin/bash

hadoop fs -rm -r /user/art/cs246/k-means/output-centroids
hadoop jar target/kmeans-1.0-SNAPSHOT.jar org.learningisfun.hadoop.Kmeans 20 /user/art/cs246/k-means/documents /user/art/cs246/k-means/random-centroids/c1.txt /user/art/cs246/k-means/output-centroids
