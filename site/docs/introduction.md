---
layout: docs
title: Introduction
permalink: /docs/introduction/
---
## Pig 

One of the big reasons that [Apache Pig](http://pig.apache.org) exists is to provide a much lower-cost entry point to
running MapReduce. Writing a MapReduce job typically ends up being expression in hundreds of lines of code to solve
problems that are often categorized as [embarrassingly parallel](http://en.wikipedia.org/wiki/Embarrassingly_parallel).
As such, these problems are typically easy to think about conceptually and can be used as a point of introspection to a
data set or combination of data sets. Thus, it doesn't make sense to write large amounts of Java code for what may be
repeated one-off questions.

## Accumulo

Accumulo is one of many storage solutions in the Apache Hadoop ecosystem, so it makes sense that Pig should also be able
to read/write data from/to Accumulo. Very similar to [Apache HBase](http://hbase.apache.org), Accumulo is a Key-Value datastore,
where a Key is made up of [multiple parts](http://accumulo.apache.org/1.5/accumulo_user_manual.html#_data_model). 

![Data Model](/images/accumulo-data-model.png)

This data model lends itself well to working with columnar data, handling changing, sparse column definitions on the fly. As
new columns are created, Accumulo can process these automatically without any user intevention. Many columns can exist
across many rows, and rows can contain different sets of columns. This can be thought of as storing a Map of data in
each row.

## File Storage

Accumulo provides many other desirable features when it comes to data management over "hand rolled" solutions using
flat-files in HDFS.

### Sorted

All data stored in Accumulo is sorted lexicographically. New data which is written to Accumulo is also written in sorted
order, while reads against Accumulo performed a merged-read against these sorted streams of data to provide a globally
sorted view over the table. This feature opens the door to many algorithms that can run efficiently over sorted data
sets.

### Tablets

Accumulo organizes data in tables. Each table is made up of multiple tablets. Each tablet contains at minimum one row
and can be composed of many files in HDFS. In practice, most tablets in Accumulo will contain many rows. As new data is
inserted into a table, Accumulo will manage how this data is written to HDFS. As new data is ingested and written to a
tablet, that tablet will eventually split from one tablet into many. As these splits occurs, Accumulo manages each of
these files in HDFS for you transparantly alleviating the necessity to implement data retention and organizational logic
in the application.

## Indexing

In addition to the trivial "map in row" layout, more advanced table schemas exist such as inverted indexes,
document-partitioned indexes, and edge lists to name a few. All of these can be expressed using the same "5 tuple" Key
data model that Accumulo provides.

Next, how to [use Pig to manipulate "map in row" datasets in Accumulo](/docs/map-storage).
