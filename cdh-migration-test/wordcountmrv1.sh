#!/bin/bash
hadoop fs -rm -r output/wordcount/mrv1

hadoop jar target/WordCount-1.0-SNAPSHOT.jar org.learningisfun.hadoop.WordCount -Dwordcount.case.sensitive=false data/gutenberg/shakespeare output/wordcount/mrv1 

