---
layout: docs
title: Air Flight Example
permalink: /docs/flight-example/
---

We can use some airplane flight information as a example to show some basic functionality that we can provide with this
Accumulo and Pig support. The [American Statistical Association](www.amstat.org) has a nice collection of [data sets
regarding flights](http://stat-computing.org/dataexpo/2009/the-data.html) from 1987 through 2008. We can use the data
set of flights and the [data set about airports](http://stat-computing.org/dataexpo/2009/supplemental-data.html) to
easily join some records in a few lines of Pig.

## Writing the data

Download at least one year of the flight data, decompress it and place it into HDFS. From this point, we can write a few
lines of Pig to read the file using PigStorage, create a unique row key and configure _AccumuloStorage_ to use the given
column names to write the data into Accumulo. We need to make sure we include the accumulo-pig jar which has the
_AccumuloStorage_ implementation we're leveraging, in addition to the Accumulo, Thrift and ZooKeeper dependencies.

<pre class="code">
register /path/to/accumulo-pig-1.4.4-SNAPSHOT.jar;
register /usr/local/lib/accumulo/lib/accumulo-core-1.4.4.jar;
register /usr/local/lib/accumulo/lib/libthrift-0.6.1.jar;
register /usr/local/lib/zookeeper/zookeeper-3.4.5.jar;
register /usr/local/lib/accumulo/lib/cloudtrace-1.4.4.jar;

DEFINE FORMAT org.apache.accumulo.pig.FORMAT();

flight_data = LOAD '/flights.csv' using PigStorage(',') as (year:int, month:int, dayofmonth:int, dayofweek:int, departure_time:int, scheduled_departure_time:int, 
        arrival_time:int, scheduled_arrival_time:int, carrier:chararray, flight_number:int, tail_number:chararray, actual_elapsed_time:int, 
        scheduled_elapsed_time:int, air_time:int, arrival_delay:int, departure_delay:int, origin:chararray, destination:chararray, 
        distance:int, taxi_in:int, taxi_out:int, cancelled:int, cancellation_code:chararray, diverted:int, carrier_delay:chararray, 
        weather_delay:chararray, nas_delay:chararray, security_delay:chararray, late_aircraft_delay:chararray);

flight_data = FOREACH flight_data GENERATE CONCAT(FORMAT('%04d-%02d-%02d', year, month, dayofmonth), CONCAT('_', CONCAT(carrier, CONCAT('_', (chararray)flight_number)))) as rowkey,
        departure_time, scheduled_departure_time, arrival_time, scheduled_arrival_time, carrier, flight_number, tail_number, actual_elapsed_time, scheduled_elapsed_time, air_time,
        arrival_delay, departure_delay, origin, destination, distance, taxi_in, taxi_out, cancelled, cancellation_code, diverted, carrier_delay, weather_delay, nas_delay,
        security_delay, late_aircraft_delay;

STORE flight_data into 'accumulo://flights?instance=accumulo&amp;user=root&amp;password=secret&amp;zookeepers=localhost' using 
        org.apache.accumulo.pig.AccumuloStorage('departure_time,scheduled_departure_time,arrival_time,scheduled_arrival_time,carrier,flight_number,tail_number,actual_elapsed_time,scheduled_elapsed_time,air_time,arrival_delay,departure_delay,origin,destination,distance,taxi_in,taxi_out,cancelled,cancellation_code,diverted,carrier_delay,weather_delay,nas_delay,security_delay,late_aircraft_delay');
</pre>

This will load a years worth of data into Accumulo with a rowkey that is the concatenation of the year, month and day of
the flight, the carrier code, and the flight number, which should give us good distribution and parallelism inside of
Accumulo.

Next, we want to do the same for the airport information:

<pre class="code">
register /path/to/accumulo-pig-1.4.4-SNAPSHOT.jar;
register /usr/local/lib/accumulo/lib/accumulo-core-1.4.4.jar;
register /usr/local/lib/accumulo/lib/libthrift-0.6.1.jar;
register /usr/local/lib/zookeeper/zookeeper-3.4.5.jar;
register /usr/local/lib/accumulo/lib/cloudtrace-1.4.4.jar;

airports = LOAD '/airports.csv' using PigStorage(',') as (code:chararray, name:chararray, city:chararray, state:chararray, country:chararray, latitude:double, longitude:double);

airports = FOREACH airports GENERATE REPLACE(code, '"', '') as code, REPLACE(name, '"', '') as name, REPLACE(city, '"', '') as city, 
        REPLACE(state, '"', '') as state, REPLACE(country, '"', '') as country, latitude, longitude;

airports = FOREACH airports GENERATE code as rowkey, code, name, city, state, country, latitude, longitude;

STORE airports into 'accumulo://airports?instance=accumulo&amp;user=root&amp;password=secret&amp;zookeepers=localhost' using 
        org.apache.accumulo.pig.AccumuloStorage('code,name,city,state,country,latitude,longitude');
</pre>

At this point, we now have flight information in the 'flight_data' Accumulo table and airport information in the
'airports' Accumulo table. We can join information about the origin airport code with the actual airport information.

<pre class="code">
<span class="comment">-- Read a reduced set of our flight data</span>
<span class="variable">flight_data</span> = <span class="keyword">LOAD</span> <span class="constants">'accumulo://flights?instance=accumulo&amp;user=pig&amp;password=password&amp;zookeepers=localhost&amp;fetch_columns=destination,departure_time,scheduled_departure_time,flight_number,taxi_in,taxi_out,origin'</span>
<span class="keyword">USING</span> org.apache.accumulo.pig.AccumuloStorage() <span class="keyword">AS</span> (rowkey:<span class="type">chararray</span>, data:<span class="type">map[]</span>);

<span class="comment">-- Also read airport information</span>
<span class="variable">airports</span> = <span class="keyword">LOAD</span> <span class="constants">'accumulo://airports?instance=accumulo&amp;user=pig&amp;password=password&amp;zookeepers=localhost'</span> <span class="keyword">USING</span>
org.apache.accumulo.pig.AccumuloStorage() <span class="keyword">AS</span> (rowkey:<span class="type">chararray</span>, data:<span class="type">map[]</span>);

<span class="comment">-- Permute the map</span>
<span class="variable">flight_data</span> = <span class="keyword">FOREACH</span> <span class="variable">flight_data</span> <span class="keyword">GENERATE</span> rowkey, data#<span class="constants">'origin'</span> <span class="keyword">AS</span> origin, data#<span class="constants">'destination'</span> <span class="keyword">AS</span> destination, data#<span class="constants">'departure_time'</span> <span class="keyword">AS</span> departure_time,
data#<span class="constants">'scheduled_departure_time'</span> <span class="keyword">AS</span> scheduled_departure_time, data#<span class="constants">'flight_number'</span> <span class="keyword">AS</span> flight_number, data#<span class="constants">'taxi_in'</span> <span class="keyword">AS</span> taxi_in, data#<span class="constants">'taxi_out'</span> <span class="keyword">AS</span> taxi_out;

<span class="comment">-- Permute the map</span>
<span class="variable">airports</span> = <span class="keyword">FOREACH</span> <span class="variable">airports</span> <span class="keyword">GENERATE</span> data#<span class="constants">'name'</span> <span class="keyword">AS</span> name, data#<span class="constants">'state'</span> <span class="keyword">AS</span> state, data#<span class="constants">'code'</span> <span class="keyword">AS</span> code, data#<span class="constants">'country'</span> <span class="keyword">AS</span> country, data#<span class="constants">'city'</span> <span class="keyword">AS</span> city;

<span class="comment">-- Add airport information about the origin of the flight</span>
<span class="variable">flights_with_origin</span> = <span class="keyword">JOIN</span> <span class="variable">flight_data</span> <span class="keyword">BY</span> origin, <span class="variable">airports</span> <span class="keyword">BY</span> code;

<span class="comment">-- Store this information back into Accumulo in a new table</span>
<span class="keyword">STORE</span> <span class="variable">flights_with_origin</span> <span class="keyword">INTO</span> <span class="constants">'accumulo://flights_with_airports?instance=accumulo1.4&amp;user=root&amp;password=secret&amp;zookeepers=localhost'</span> \
<span class="keyword">USING</span> org.apache.accumulo.pig.AccumuloStorage(<span class="constants">'origin,destination,departure_time,scheduled_departure_time,flight_number,taxi_in,taxi_out,name,state,code,country,city'</span>);
</pre>
