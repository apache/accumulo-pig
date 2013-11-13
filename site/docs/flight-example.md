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
<span class="keyword">REGISTER</span> <span class="constants">/path/to/accumulo-pig-1.4.4-SNAPSHOT.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/accumulo-core-1.4.4.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/libthrift-0.6.1.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/zookeeper/zookeeper-3.4.5.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/cloudtrace-1.4.4.jar;</span>

<span class="keyword">DEFINE</span> FORMAT org.apache.accumulo.pig.FORMAT();

<span class="comment">-- Read the flight information from the csv file</span>
<span class="variable">flight_data</span> = <span class="keyword">LOAD</span> <span class="constants">'/flights.csv'</span> <span class="keyword">USING</span> PigStorage(<span class="constants">','</span>) <span class="keyword">AS</span> (year:<span class="type">int</span>, month:<span class="type">int</span>, dayofmonth:<span class="type">int</span>, dayofweek:<span class="type">int</span>, departure_time:<span class="type">int</span>, scheduled_departure_time:<span class="type">int</span>, 
        arrival_time:<span class="type">int</span>, scheduled_arrival_time:<span class="type">int</span>, carrier:<span class="type">chararray</span>, flight_number:<span class="type">int</span>, tail_number:<span class="type">chararray</span>, actual_elapsed_time:<span class="type">int</span>, 
        scheduled_elapsed_time:<span class="type">int</span>, air_time:<span class="type">int</span>, arrival_delay:<span class="type">int</span>, departure_delay:<span class="type">int</span>, origin:<span class="type">chararray</span>, destination:<span class="type">chararray</span>, 
        distance:<span class="type">int</span>, taxi_in:<span class="type">int</span>, taxi_out:<span class="type">int</span>, cancelled:<span class="type">int</span>, cancellation_code:<span class="type">chararray</span>, diverted:<span class="type">int</span>, carrier_delay:<span class="type">chararray</span>, 
        weather_delay:<span class="type">chararray</span>, nas_delay:<span class="type">chararray</span>, security_delay:<span class="type">chararray</span>, late_aircraft_delay:<span class="type">chararray</span>);

<span class="comment">-- Create a custom rowkey </span>
<span class="variable">flight_data</span> = <span class="keyword">FOREACH</span> <span class="variable">flight_data</span> <span class="keyword">GENERATE</span> <span class="keyword">CONCAT</span>(FORMAT(<span class="constants">'%04d-%02d-%02d'</span>, year, month, dayofmonth), <span class="keyword">CONCAT</span>(<span class="constants">'_'</span>, <span class="keyword">CONCAT</span>(carrier, <span class="keyword">CONCAT</span>(<span class="constants">'_'</span>, (<span class="type">chararray</span>)flight_number)))) <span class="keyword">AS</span> rowkey,
        departure_time, scheduled_departure_time, arrival_time, scheduled_arrival_time, carrier, flight_number, tail_number, actual_elapsed_time, scheduled_elapsed_time, air_time,
        arrival_delay, departure_delay, origin, destination, distance, taxi_in, taxi_out, cancelled, cancellation_code, diverted, carrier_delay, weather_delay, nas_delay,
        security_delay, late_aircraft_delay;

<span class="comment">-- Store the flights into Accumulo with the provided column names</span>
<span class="keyword">STORE</span> <span class="variable">flight_data</span> <span class="keyword">INTO</span> <span class="constants">'accumulo://flights?instance=accumulo&amp;user=pig&amp;password=secret&amp;zookeepers=localhost'</span> <span class="keyword">USING</span>
        org.apache.accumulo.pig.AccumuloStorage(<span class="constants">'departure_time,scheduled_departure_time,arrival_time,scheduled_arrival_time,carrier,flight_number,tail_number,actual_elapsed_time,scheduled_elapsed_time,air_time,arrival_delay,departure_delay,origin,destination,distance,taxi_in,taxi_out,cancelled,cancellation_code,diverted,carrier_delay,weather_delay,nas_delay,security_delay,late_aircraft_delay'</span>);
</pre>

This will load a years worth of data into Accumulo with a rowkey that is the concatenation of the year, month and day of
the flight, the carrier code, and the flight number, which should give us good distribution and parallelism inside of
Accumulo.

Next, we want to do the same for the airport information:

<pre class="code">
<span class="keyword">REGISTER</span> <span class="constants">/path/to/accumulo-pig-1.4.4-SNAPSHOT.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/accumulo-core-1.4.5-SNAPSHOT.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/libthrift-0.6.1.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/zookeeper/zookeeper-3.4.5.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/cloudtrace-1.4.5-SNAPSHOT.jar;</span>

<span class="comment">-- Read the aiport information from the CSV</span>
<span class="variable">airports</span> = <span class="keyword">LOAD</span> <span class="constants">'/airports.csv'</span> <span class="keyword">USING</span> PigStorage(<span class="constants">','</span>) <span class="keyword">AS</span> (code:<span class="type">chararray</span>, name:<span class="type">chararray</span>, city:<span class="type">chararray</span>, state:<span class="type">chararray</span>, country:<span class="type">chararray</span>, latitude:<span class="type">double</span>, longitude:<span class="type">double</span>);
  
<span class="comment">-- Cleanse the records</span>
<span class="variable">airports</span> = <span class="keyword">FOREACH</span> <span class="variable">airports</span> <span class="keyword">GENERATE</span> <span class="keyword">REPLACE</span>(code, <span class="constants">'"'</span>, <span class="constants">''</span>) <span class="keyword">AS</span> code, <span class="keyword">REPLACE</span>(name, <span class="constants">'"'</span>, <span class="constants">''</span>) <span class="keyword">AS</span> name, <span class="keyword">REPLACE</span>(city, <span class="constants">'"'</span>, <span class="constants">''</span>) <span class="keyword">AS</span> city, 
        <span class="keyword">REPLACE</span>(state, <span class="constants">'"'</span>, <span class="constants">''</span>) <span class="keyword">AS</span> state, <span class="keyword">REPLACE</span>(country, <span class="constants">'"'</span>, <span class="constants">''</span>) <span class="keyword">AS</span> country, latitude, longitude;

<span class="comment">-- Create a rowkey</span>
<span class="variable">airports</span> = <span class="keyword">FOREACH</span> <span class="variable">airports</span> <span class="keyword">GENERATE</span> code <span class="keyword">AS</span> rowkey, code, name, city, state, country, latitude, longitude;

<span class="comment">-- Write the airport data to Accumulo with the provided column names</span>
<span class="keyword">STORE</span> <span class="variable">airports</span> <span class="keyword">INTO</span> <span class="constants">'accumulo://airports?instance=accumulo&amp;user=pig&amp;password=secret&amp;zookeepers=localhost'</span> <span class="keyword">USING</span> 
        org.apache.accumulo.pig.AccumuloStorage(<span class="constants">'code,name,city,state,country,latitude,longitude'</span>);
</pre>

At this point, we now have flight information in the 'flight_data' Accumulo table and airport information in the
'airports' Accumulo table. We can project our flight data down to just departure flight information and join this
information about the origin airport code with the actual airport information.

<pre class="code">
<span class="keyword">REGISTER</span> <span class="constants">/path/to/accumulo-pig-1.4.4-SNAPSHOT.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/accumulo-core-1.4.5-SNAPSHOT.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/libthrift-0.6.1.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/zookeeper/zookeeper-3.4.5.jar;</span>
<span class="keyword">REGISTER</span> <span class="constants">/usr/local/lib/accumulo/lib/cloudtrace-1.4.5-SNAPSHOT.jar;</span>

<span class="comment">-- Read a reduced set of our flight data</span>
<span class="variable">flight_data</span> = <span class="keyword">LOAD</span> <span class="constants">'accumulo://flights?instance=accumulo&amp;user=pig&amp;password=password&amp;zookeepers=localhost&amp;fetch_columns=departure_time,scheduled_departure_time,flight_number,taxi_out,origin&amp;start=2001&amp;end=2003'</span>
<span class="keyword">USING</span> org.apache.accumulo.pig.AccumuloStorage() <span class="keyword">AS</span> (rowkey:<span class="type">chararray</span>, data:<span class="type">map[]</span>);

<span class="comment">-- Also read airport information</span>
<span class="variable">airports</span> = <span class="keyword">LOAD</span> <span class="constants">'accumulo://airports?instance=accumulo&amp;user=pig&amp;password=password&amp;zookeepers=localhost'</span> <span class="keyword">USING</span>
org.apache.accumulo.pig.AccumuloStorage() <span class="keyword">AS</span> (rowkey:<span class="type">chararray</span>, data:<span class="type">map[]</span>);

<span class="comment">-- Permute the map of flight data</span>
<span class="variable">flight_data</span> = <span class="keyword">FOREACH</span> <span class="variable">flight_data</span> <span class="keyword">GENERATE</span> rowkey, data#<span class="constants">'origin'</span> <span class="keyword">AS</span> origin, data#<span class="constants">'departure_time'</span> <span class="keyword">AS</span> departure_time,
data#<span class="constants">'scheduled_departure_time'</span> <span class="keyword">AS</span> scheduled_departure_time, data#<span class="constants">'flight_number'</span> <span class="keyword">AS</span> flight_number, data#<span class="constants">'taxi_out'</span> <span class="keyword">AS</span> taxi_out;

<span class="comment">-- Permute the map of airport data</span>
<span class="variable">airports</span> = <span class="keyword">FOREACH</span> <span class="variable">airports</span> <span class="keyword">GENERATE</span> data#<span class="constants">'name'</span> <span class="keyword">AS</span> name, data#<span class="constants">'state'</span> <span class="keyword">AS</span> state, data#<span class="constants">'code'</span> <span class="keyword">AS</span> code, data#<span class="constants">'country'</span> <span class="keyword">AS</span> country, data#<span class="constants">'city'</span> <span class="keyword">AS</span> city;

<span class="comment">-- Add airport information about the origin of the flight</span>
<span class="variable">flights_with_origin</span> = <span class="keyword">JOIN</span> <span class="variable">flight_data</span> <span class="keyword">BY</span> origin, <span class="variable">airports</span> <span class="keyword">BY</span> code;

<span class="comment">-- Store this information back into Accumulo in a new table</span>
<span class="keyword">STORE</span> <span class="variable">flights_with_origin</span> <span class="keyword">INTO</span> <span class="constants">'accumulo://flights_with_airports?instance=accumulo&amp;user=pig&amp;password=secret&amp;zookeepers=localhost'</span>
        <span class="keyword">USING</span> org.apache.accumulo.pig.AccumuloStorage(<span class="constants">'origin,departure_time,scheduled_departure_time,flight_number,taxi_out,name,state,code,country,city'</span>);
</pre>

Opening the Accumulo shell to view the data which we have just written, we can see some sample records key values:

<pre>
pig@accumulo flights_with_airports> scan
2000-01-01_AA_1 city: []    New York
2000-01-01_AA_1 code: []    JFK
2000-01-01_AA_1 country: []    USA
2000-01-01_AA_1 departure_time: []    859
2000-01-01_AA_1 destination: []    LAX
2000-01-01_AA_1 flight_number: []    1
2000-01-01_AA_1 name: []    John F Kennedy Intl
2000-01-01_AA_1 origin: []    JFK
2000-01-01_AA_1 scheduled_departure_time: []    900
2000-01-01_AA_1 state: []    NY
2000-01-01_AA_1 taxi_in: []    6
2000-01-01_AA_1 taxi_out: []    45
2000-01-01_AA_10 city: []    Los Angeles
2000-01-01_AA_10 code: []    LAX
2000-01-01_AA_10 country: []    USA
2000-01-01_AA_10 departure_time: []    2200
2000-01-01_AA_10 destination: []    JFK
2000-01-01_AA_10 flight_number: []    10
2000-01-01_AA_10 name: []    Los Angeles International
2000-01-01_AA_10 origin: []    LAX
2000-01-01_AA_10 scheduled_departure_time: []    2200
2000-01-01_AA_10 state: []    CA
2000-01-01_AA_10 taxi_in: []    8
2000-01-01_AA_10 taxi_out: []    17
</pre>
