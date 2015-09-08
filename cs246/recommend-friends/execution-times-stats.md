# Execution Time Statistics
| Solution                   | Average Execution Time (in secs) |
|:---------------------------|:--------------------------------:|
| Stanford's Hadoop Solution |              100.67              |
| Solution 1 Spark/Scala     |              146.67              |
| Solution 1 pySpark         |               286                |
| Solution 2 pySpark         |              494.33              |

Each implementation was ran three times. The Stanford Hadoop solution was downloaded from Stanford's CS246 site. 

Execution Environment

1. A pseudo-distributed YARN using Cloudera's CDH 5.4.4 distribution.
2. Apache Spark 1.4.1 (compiled from source).
3. Yosemite OS, [Macbook Pro mid-2014, 2.5 GHz](https://support.apple.com/kb/SP704?locale=en_US).

For those interested in running CDH natively in the Mac, check out Cloudera's [blog](http://blog.cloudera.com/blog/2014/09/how-to-install-cdh-on-mac-osx-10-9-mavericks/).
