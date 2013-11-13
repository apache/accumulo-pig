---
layout: docs
title: Using Map Storage
permalink: /docs/map-storage/
---
## Connection

The _AccumuloStorage_ class provides the ability to read data from an Accumulo table. The string argument to the
[LOAD](http://pig.apache.org/docs/r0.12.0/basic.html#load) command is a URI which contains Accumulo connection
information and some query options. The URI scheme is always "accumulo" and the path is the Accumulo table name to read
from. The query string is used to provide the previously mentioned connection and query options. These options are the
same regardless of whether _AccumuloStorage_ is being used for reading or writing.

* `instance` - The Accumulo instance name
* `user` - The Accumulo user
* `password` - The password for the Accumulo user
* `zookeepers` -  A comma separated list of ZooKeeper hosts for the Accumulo instance

## Reading

Some basic Accumulo read parameters are exposed for use. All of the following are optional.

* `fetch_columns` - A comma separated list of optionally colon-separated elements mapping to column family and qualifier
pairs, e.g. `foo:bar,column1,column5`. **Default: All columns**.
* `begin` - The row to begin scanning from. **Default: beginning of the table (null)**.
* `end` - The row to stop scanning at. **Default: end of the table (null)**.
* `auths` - A comma separated list of Authorizations to use for the provided users. **Default: all authorizations the
user has**.

_AccumuloStorage_ will return you data in the following schema. 

<pre class="code">
(rowkey:<span class="type">chararray</span>, data:<span class="type">map[]</span>)
</pre>

Each key in the map is a column (family and qualifier) within the provided rowkey and the values are the Accumulo values for the given
rowkey+column. By default, the map key will have a colon separator between the column family and column qualifier. A
boolean argument can be provided to the _AccumuloStorage_ constructor. If this boolean is true, the map key will only be
composed of the column qualifier and each map will be the collection of each column family within the row. For example:

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>measurements</td><td>height</td><td>72inches</td></tr>
    <tr><td>1</td><td>measurements</td><td>weight</td><td>180lbs</td></tr>
    <tr><td>1</td><td>location</td><td>city</td><td>San Francisco</td></tr>
    <tr><td>1</td><td>location</td><td>state</td><td>California</td></tr>
</table>

By default will generate a tuple of the following:

<pre class="code">
(1, [measurements:height#72inches, measurements:weight#180lbs, location:city#San Francisco, location:state#California])
</pre>

If the previously mentioned boolean argument is provided as true, the following will be generated instead:

<pre class="code">
(1, [measurements:height#72inches, measurements:weight#180lbs], [location:city#San Francisco, location:state#California])
</pre>

## Writing

Some basic Accumulo write parameters are exposed for use. Like read operations, all of the following are optional.

* `write_buffer_size` - The size, in bytes, to buffer Mutations before sending to an Accumulo server. **Default:
10,000,000 (10MB)**.
* `write_threads` - The number of threads to use when sending Mutations to Accumulo servers. **Default: 10**.
* `write_latency_ms` - The number of milliseconds to wait before forcibly flushing Mutations to Accumulo. **Default:
10,000 (10 seconds)**.

The _AccumuloStorage_ class can accept data in a few different formats. A String argument may be provided to the
_AccumuloStorage_ constructor to provide a column mapping which is a comma-separated list (this will be touched on
later). One thing that is universal is that the first entry in the tuple is treated as the rowkey and must be
castable to a _chararray_. For elements 1 through _N_ in a Tuple, the two cases may apply.

### Data as map

When the Tuple entry is a map, we can naturally treat it as a column to value mapping, placing the map key in the column
family and the map value in the Accumulo value. When a non-empty value from the column mapping provided in the
_AccumuloStorage_ constructor is present, we will use the _N_th value in that CSV as a column family for this _N_th
entry in the Tuple and the map key is placed in the qualifier.

If the entry from the column mapping happens to contain a colon, _AccumuloStorage_ will split the key on the colon. In
the case where the colon is present, the characters following the colon in the column mapping entry will be placed in
the column qualifier with the map key being append to it.

Concretely, let's say we have the following Tuple:

<pre class="code">
(1, [height#72inches, weight#180lbs, city#San Francisco, state#California])
</pre>

With an empty (or null) column mapping, _AccumuloStorage_ will generate the following Key-Value pairs:

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>height</td><td></td><td>72inches</td></tr>
    <tr><td>1</td><td>weight</td><td></td><td>180lbs</td></tr>
    <tr><td>1</td><td>city</td><td></td><td>San Francisco</td></tr>
    <tr><td>1</td><td>state</td><td></td><td>California</td></tr>
</table>

With a column mapping of "information":

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>information</td><td>height</td><td>72inches</td></tr>
    <tr><td>1</td><td>information</td><td>weight</td><td>180lbs</td></tr>
    <tr><td>1</td><td>information</td><td>city</td><td>San Francisco</td></tr>
    <tr><td>1</td><td>information</td><td>state</td><td>California</td></tr>
</table>

And, with a column mapping of "information:person\_":

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>information</td><td>person_height</td><td>72inches</td></tr>
    <tr><td>1</td><td>information</td><td>person_weight</td><td>180lbs</td></tr>
    <tr><td>1</td><td>information</td><td>person_city</td><td>San Francisco</td></tr>
    <tr><td>1</td><td>information</td><td>person_state</td><td>California</td></tr>
</table>

### Data as fields 

When an entry in the Tuple is not a map, we require a non-empty column mapping to use in coordination with the current
field. The same colon-delimiter logic that was described when handling a Map in a tuple applies with other fields.

<pre class="code">
(1, 72inches, 180lbs, San Francisco, California)
</pre>

With an empty (or null) column mapping, _AccumuloStorage_ will generate the following Key-Value pairs:

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>height</td><td></td><td>72inches</td></tr>
    <tr><td>1</td><td>weight</td><td></td><td>180lbs</td></tr>
    <tr><td>1</td><td>city</td><td></td><td>San Francisco</td></tr>
    <tr><td>1</td><td>state</td><td></td><td>California</td></tr>
</table>

With a column mapping of "information":

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>information</td><td>height</td><td>72inches</td></tr>
    <tr><td>1</td><td>information</td><td>weight</td><td>180lbs</td></tr>
    <tr><td>1</td><td>information</td><td>city</td><td>San Francisco</td></tr>
    <tr><td>1</td><td>information</td><td>state</td><td>California</td></tr>
</table>

And, with a column mapping of "information:person\_":

<table border="1">
    <tr><th>Row</th><th>ColumnFamily</th><th>ColumnQualifier</th><th>Value</th></tr>
    <tr><td>1</td><td>information</td><td>person_height</td><td>72inches</td></tr>
    <tr><td>1</td><td>information</td><td>person_weight</td><td>180lbs</td></tr>
    <tr><td>1</td><td>information</td><td>person_city</td><td>San Francisco</td></tr>
    <tr><td>1</td><td>information</td><td>person_state</td><td>California</td></tr>
</table>

In addition to dealing with data in this row with columns approach, you can also treat read/write data from Accumulo
with Pig in terms of [keys and values]({{site.url}}/docs/key-value-storage).
