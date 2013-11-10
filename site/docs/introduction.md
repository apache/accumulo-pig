---
layout: default
title: Accumulo Storage with Pig
permalink: /docs/introduction/
---
[Apache Accumulo](http://accumulo.apache.org) 

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

<p> Vestibulum vulputate nisi non imperdiet elementum. Pellentesque at
consequat nisi. Fusce ut luctus justo. Aenean tincidunt ut risus
condimentum convallis. Praesent eget tristique risus. Cras pellentesque sed
libero ac elementum. Quisque tempus commodo neque, laoreet accumsan lectus
sollicitudin eget. In convallis neque nisi, a iaculis neque interdum ac.
Suspendisse in ante lacinia dolor faucibus auctor.
</p>

<p>Nulla fringilla quis turpis a gravida. Quisque tellus arcu, sagittis et sapien
ut, imperdiet scelerisque est. Duis sapien mi, elementum vitae sem quis, varius
tincidunt tortor. In commodo semper magna. Donec ultrices nunc est, nec
volutpat leo porta scelerisque. Praesent tellus leo, scelerisque eget tortor
eget, posuere sodales nulla. Mauris imperdiet magna eget tristique consequat.
Nullam adipiscing at arcu in vestibulum. Donec consectetur justo sed odio
vehicula, vel lobortis libero vehicula. Fusce rutrum justo lorem, sed bibendum
ipsum ultrices eget. Praesent lobortis justo quis sem adipiscing rutrum ac eget
nisi. Pellentesque et justo in leo rutrum rhoncus a ut neque. Fusce faucibus,
orci nec venenatis dapibus, est leo ornare eros, ac adipiscing erat felis sit
amet tellus. Nulla vehicula ipsum sit amet accumsan tempor.
</p>

<p>Nulla ac est tincidunt, lacinia quam nec, mollis ante. Nulla ut tincidunt
massa, vel laoreet elit. Aliquam erat volutpat. Mauris varius dolor in eros
blandit adipiscing. Nam ultrices tellus quam, eu porta quam varius ac.
Phasellus in massa fringilla, mattis nisi vel, condimentum diam. Cras porttitor
eget arcu vel tempor.
</p>

<p>Ut id vestibulum lorem. Fusce vitae metus sed magna tincidunt vestibulum. Fusce
in eros ac nulla vestibulum venenatis vitae vitae nisi. Donec elementum neque
ac viverra cursus. Morbi tincidunt venenatis tellus, id facilisis nibh viverra
eget. Aenean pellentesque gravida orci, sed elementum nisl vulputate at.
Suspendisse ut orci vitae tortor viverra egestas id scelerisque ante. Praesent
vel tempor justo, id tempor lacus. Proin convallis vehicula mauris. Suspendisse
tincidunt et libero vitae condimentum. Nam arcu urna, sollicitudin nec diam
congue, ultricies hendrerit mi. Vivamus viverra elit in libero rutrum commodo.
Ut eget varius arcu, ac venenatis tellus. Quisque rutrum blandit velit in
sollicitudin. Maecenas nibh purus, consectetur at elementum at, dictum et
dolor. 
</p>
