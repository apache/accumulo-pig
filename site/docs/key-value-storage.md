---
layout: docs
title: Using Key-Value Storage
permalink: /docs/key-value-storage/
---

Another method of interacting with Accumulo through Pig is through the _AccumuloKVStorage_ class. Instead of interacting
with data in terms of the rows with sparse columns as _AccumuloStorage_ provides, _AccumuloKVStorage_ provides more
"native" key-value support as the name implies. The first _N_ entries in a Tuple create the Accumulo Key while the final
entry in the Tuple is placed into the Accumulo Value.

The same configuration options that exist for _AccumuloStorage_ also exist for _AccumuloKVStorage_.

## Reading

Reading data from Accumulo using the _AccumuloKVStorage_ class is relatively simple. You will always receive a 6-Tuple
in Pig which corresponds to the 5-tuple of the Accumulo Key plus the Accumulo Value. For example, the following Accumulo
Key

<pre class="code">
row1 colfam_1:colqual_1 [foo&amp;bar] 1384307084 value
</pre>

would create the following Tuple:

<pre class="code">
("row1", "colfam_1", "colqual_1", "foo&amp;bar", 1384307084, "value")
</pre>

All elements except the timestamp, the 5th element in the Tuple, are _chararray_'s with the timestamp being a _long_.

## Writing

When writing data using the _AccumuloKVStorage_, you can provide a Tuple which contains at least four elements. Elements
one through three translate to the row, column family and column qualifier. With four elements, the fourth is the
Accumulo Value. With five, the fourth element is the column visibility and the fifth is the Accumulo Value, and with six
elements, fourth is the column visibility, fifth is the timestamp and the sixth is the Accumulo Value. For Tuples
greater than six elements in size, the extra elements are ignored.

A 4-Tuple

<pre class="code">
(row1 colfam_1, colqual_1, value)
</pre>

translates to 

<pre class="code">
row1 colfam_1:colqual_1 [] Long.MAX_VALUE value
</pre>

A 5-Tuple

<pre class="code">
(row1 colfam_1, colqual_1, column_vis, value)
</pre>

translates to 

<pre class="code">
row1 colfam_1:colqual_1 [column_vis] Long.MAX_VALUE value
</pre>

And a 6-Tuple

<pre class="code">
(row1 colfam_1, colqual_1, column_vis, timestamp, value)
</pre>

translates to 

<pre class="code">
row1 colfam_1:colqual_1 [column_vis] timestamp value
</pre>

Overall, this provides a much lower-level support that may be of use in specific applications which already have data
stored in Accumulo tables. An end-to-end example can provide some basic insight to [what is currently
possible]({{site.url}}/docs/flight-example).
