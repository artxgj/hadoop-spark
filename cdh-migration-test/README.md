## CDH4 to CDH5 Migration

At a previous employer, I worked on migrating its hadoop distribution version from CDH 4.3 to CDH 5.7 (huge jump!)

This is a simple test to compile as-is a Map-Reduce version 1 program and run it in YARN. The test is conducted using CDH5, Cloudera's Hadoop Distribution with YARN.

The test is accomplished by creating a pom.xml file that uses the [CDH5 Maven Repository](http://www.cloudera.com/content/cloudera/en/documentation/core/latest/topics/cdh_vd_cdh5_maven_repo.html#concept_wbl_iwn_yk_unique_2 "Title") and CDH5 mr1 classes.
