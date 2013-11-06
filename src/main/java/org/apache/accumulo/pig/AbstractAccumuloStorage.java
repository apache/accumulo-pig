/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.accumulo.pig;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.joda.time.DateTime;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Accumulo
 * 
 * A Key/Val pair will be returned as tuples: (key, colfam, colqual, colvis, timestamp, value). All fields except timestamp are DataByteArray, timestamp is a
 * long.
 * 
 * Tuples can be written in 2 forms: (key, colfam, colqual, colvis, value) OR (key, colfam, colqual, value)
 * 
 */
public abstract class AbstractAccumuloStorage extends LoadFunc implements StoreFuncInterface {
  private static final Log LOG = LogFactory.getLog(AbstractAccumuloStorage.class);
  
  private static final String COLON = ":", COMMA = ",";
  
  private Configuration conf;
  private RecordReader<Key,Value> reader;
  private RecordWriter<Text,Mutation> writer;
  
  String inst;
  String zookeepers;
  String user;
  String password;
  String table;
  Text tableName;
  String auths;
  Authorizations authorizations;
  List<Pair<Text,Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text,Text>>();
  
  String start = null;
  String end = null;
  
  int maxWriteThreads = 10;
  long maxMutationBufferSize = 10 * 1000 * 1000;
  int maxLatency = 10 * 1000;
  
  protected LoadStoreCaster caster;
  protected ResourceSchema schema;
  protected String contextSignature = null;
  
  public AbstractAccumuloStorage() {}
  
  @Override
  public Tuple getNext() throws IOException {
    try {
      // load the next pair
      if (!reader.nextKeyValue())
        return null;
      
      Key key = (Key) reader.getCurrentKey();
      Value value = (Value) reader.getCurrentValue();
      assert key != null && value != null;
      return getTuple(key, value);
    } catch (InterruptedException e) {
      throw new IOException(e.getMessage());
    }
  }
  
  protected abstract Tuple getTuple(Key key, Value value) throws IOException;
  
  @Override
  @SuppressWarnings("rawtypes")
  public InputFormat getInputFormat() {
    return new AccumuloInputFormat();
  }
  
  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void prepareToRead(RecordReader reader, PigSplit split) {
    this.reader = reader;
  }
  
  private void setLocationFromUri(String location) throws IOException {
    // ex:
    // accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&fetch_columns=col1:cq1,col2:cq2&start=abc&end=z
    String columns = "";
    try {
      if (!location.startsWith("accumulo://"))
        throw new Exception("Bad scheme.");
      String[] urlParts = location.split("\\?");
      if (urlParts.length > 1) {
        for (String param : urlParts[1].split("&")) {
          String[] pair = param.split("=");
          if (pair[0].equals("instance"))
            inst = pair[1];
          else if (pair[0].equals("user"))
            user = pair[1];
          else if (pair[0].equals("password"))
            password = pair[1];
          else if (pair[0].equals("zookeepers"))
            zookeepers = pair[1];
          else if (pair[0].equals("auths"))
            auths = pair[1];
          else if (pair[0].equals("fetch_columns"))
            columns = pair[1];
          else if (pair[0].equals("start"))
            start = pair[1];
          else if (pair[0].equals("end"))
            end = pair[1];
          else if (pair[0].equals("write_buffer_size_bytes"))
            maxMutationBufferSize = Long.parseLong(pair[1]);
          else if (pair[0].equals("write_threads"))
            maxWriteThreads = Integer.parseInt(pair[1]);
          else if (pair[0].equals("write_latency_ms"))
            maxLatency = Integer.parseInt(pair[1]);
        }
      }
      String[] parts = urlParts[0].split("/+");
      table = parts[1];
      tableName = new Text(table);
      
      if (auths == null || auths.equals("")) {
        authorizations = new Authorizations();
      } else {
        authorizations = new Authorizations(auths.split(COMMA));
      }
      
      if (!StringUtils.isEmpty(columns)) {
        for (String cfCq : columns.split(COMMA)) {
          if (cfCq.contains(COLON)) {
            String[] c = cfCq.split(COLON);
            columnFamilyColumnQualifierPairs.add(new Pair<Text,Text>(new Text(c[0]), new Text(c[1])));
          } else {
            columnFamilyColumnQualifierPairs.add(new Pair<Text,Text>(new Text(cfCq), null));
          }
        }
      }
      
    } catch (Exception e) {
      throw new IOException(
          "Expected 'accumulo://<table>[?instance=<instanceName>&user=<user>&password=<password>&zookeepers=<zookeepers>&auths=<authorizations>&"
              + "[start=startRow,end=endRow,fetch_columns=[cf1:cq1,cf2:cq2,...],write_buffer_size_bytes=10000000,write_threads=10,write_latency_ms=30000]]': "
              + e.getMessage());
    }
  }
  
  protected RecordWriter<Text,Mutation> getWriter() {
    return writer;
  }
  
  @Override
  public void setLocation(String location, Job job) throws IOException {
    conf = job.getConfiguration();
    setLocationFromUri(location);
    
    int sequence = AccumuloInputFormat.nextSequence();
    
    // TODO Something more.. "certain".
    if (conf.getBoolean(AccumuloInputFormat.class.getSimpleName() + ".configured." + sequence, false)) {
      LOG.warn("InputFormat already configured for " + sequence);
      return;
      // throw new RuntimeException("Was provided sequence number which was already configured: " + sequence);
    }
    
    AccumuloInputFormat.setInputInfo(conf, sequence, user, password.getBytes(), table, authorizations);
    AccumuloInputFormat.setZooKeeperInstance(conf, sequence, inst, zookeepers);
    if (columnFamilyColumnQualifierPairs.size() > 0) {
      LOG.info("columns: " + columnFamilyColumnQualifierPairs);
      AccumuloInputFormat.fetchColumns(conf, sequence, columnFamilyColumnQualifierPairs);
    }
    
    Collection<Range> ranges = Collections.singleton(new Range(start, end));
    
    LOG.info("Scanning Accumulo for " + ranges);
    
    AccumuloInputFormat.setRanges(conf, sequence, ranges);
    
    configureInputFormat(conf, sequence);
  }
  
  protected void configureInputFormat(Configuration conf, int sequence) {
    
  }
  
  protected void configureOutputFormat(Configuration conf) {
    
  }
  
  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }
  
  @Override
  public void setUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }
  
  /* StoreFunc methods */
  public void setStoreFuncUDFContextSignature(String signature) {
    this.contextSignature = signature;
    
  }
  
  /**
   * Returns UDFProperties based on <code>contextSignature</code>.
   */
  protected Properties getUDFProperties() {
    return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] {contextSignature});
  }
  
  public String relToAbsPathForStoreLocation(String location, Path curDir) throws IOException {
    return relativeToAbsolutePath(location, curDir);
  }
  
  public void setStoreLocation(String location, Job job) throws IOException {
    conf = job.getConfiguration();
    setLocationFromUri(location);
    
    int sequence = AccumuloOutputFormat.nextSequence();
    
    // TODO Something more.. "certain".
    if (conf.getBoolean(AccumuloOutputFormat.class.getSimpleName() + ".configured." + sequence, false)) {
      LOG.warn("OutputFormat already configured for " + sequence);
      return;
      // throw new RuntimeException("Was provided sequence number which was already configured: " + sequence);
    }
    
    AccumuloOutputFormat.setOutputInfo(conf, sequence, user, password.getBytes(), true, table);
    AccumuloOutputFormat.setZooKeeperInstance(conf, sequence, inst, zookeepers);
    AccumuloOutputFormat.setMaxLatency(conf, sequence, maxLatency);
    AccumuloOutputFormat.setMaxMutationBufferSize(conf, sequence, maxMutationBufferSize);
    AccumuloOutputFormat.setMaxWriteThreads(conf, sequence, maxWriteThreads);
    
    configureOutputFormat(conf);
  }
  
  @SuppressWarnings("rawtypes")
  public OutputFormat getOutputFormat() {
    return new AccumuloOutputFormat();
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void prepareToWrite(RecordWriter writer) {
    this.writer = writer;
  }
  
  public abstract Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException;
  
  public void putNext(Tuple tuple) throws ExecException, IOException {
    Collection<Mutation> muts = getMutations(tuple);
    for (Mutation mut : muts) {
      try {
        getWriter().write(tableName, mut);
      } catch (InterruptedException e) {
        throw new IOException(e);
      }
    }
  }
  
  public void cleanupOnFailure(String failure, Job job) {}
  
  public void cleanupOnSuccess(String location, Job job) {}
  
  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    if (!(caster instanceof LoadStoreCaster)) {
      LOG.error("Caster must implement LoadStoreCaster for writing to Accumulo.");
      throw new IOException("Bad Caster " + caster.getClass());
    }
    schema = s;
    getUDFProperties().setProperty(contextSignature + "_schema", ObjectSerializer.serialize(schema));
  }
  
  protected Text tupleToText(Tuple tuple, int i, ResourceFieldSchema[] fieldSchemas) throws IOException {
    Object o = tuple.get(i);
    byte type = schemaToType(o, i, fieldSchemas);
    
    return objToText(o, type);
  }
  
  protected Text objectToText(Object o, ResourceFieldSchema fieldSchema) throws IOException {
    byte type = schemaToType(o, fieldSchema);
    
    return objToText(o, type);
  }
  
  protected byte schemaToType(Object o, ResourceFieldSchema fieldSchema) {
    return (fieldSchema == null) ? DataType.findType(o) : fieldSchema.getType();
  }
  
  protected byte schemaToType(Object o, int i, ResourceFieldSchema[] fieldSchemas) {
    return (fieldSchemas == null) ? DataType.findType(o) : fieldSchemas[i].getType();
  }
  
  protected byte[] tupleToBytes(Tuple tuple, int i, ResourceFieldSchema[] fieldSchemas) throws IOException {
    Object o = tuple.get(i);
    byte type = schemaToType(o, i, fieldSchemas);
    
    return objToBytes(o, type);
    
  }
  
  protected long objToLong(Tuple tuple, int i, ResourceFieldSchema[] fieldSchemas) throws IOException {
    Object o = tuple.get(i);
    byte type = schemaToType(o, i, fieldSchemas);
    
    switch (type) {
      case DataType.LONG:
        return (Long) o;
      case DataType.CHARARRAY:
        String timestampString = (String) o;
        try {
          return Long.parseLong(timestampString);
        } catch (NumberFormatException e) {
          final String msg = "Could not cast chararray into long: " + timestampString;
          LOG.error(msg);
          throw new IOException(msg, e);
        }
      case DataType.DOUBLE:
        Double doubleTimestamp = (Double) o;
        return doubleTimestamp.longValue();
      case DataType.FLOAT:
        Float floatTimestamp = (Float) o;
        return floatTimestamp.longValue();
      case DataType.INTEGER:
        Integer intTimestamp = (Integer) o;
        return intTimestamp.longValue();
      case DataType.BIGINTEGER:
        BigInteger bigintTimestamp = (BigInteger) o;
        long longTimestamp = bigintTimestamp.longValue();
        
        BigInteger recreatedTimestamp = BigInteger.valueOf(longTimestamp);
        
        if (!recreatedTimestamp.equals(bigintTimestamp)) {
          LOG.warn("Downcasting BigInteger into Long results in a change of the original value. Was " + bigintTimestamp + " but is now " + longTimestamp);
        }
        
        return longTimestamp;
      case DataType.BIGDECIMAL:
        BigDecimal bigdecimalTimestamp = (BigDecimal) o;
        try {
          return bigdecimalTimestamp.longValueExact();
        } catch (ArithmeticException e) {
          long convertedLong = bigdecimalTimestamp.longValue();
          LOG.warn("Downcasting BigDecimal into Long results in a loss of information. Was " + bigdecimalTimestamp + " but is now " + convertedLong);
          return convertedLong;
        }
      case DataType.BYTEARRAY:
        DataByteArray bytes = (DataByteArray) o;
        try {
          return Long.parseLong(bytes.toString());
        } catch (NumberFormatException e) {
          final String msg = "Could not cast bytes into long: " + bytes.toString();
          LOG.error(msg);
          throw new IOException(msg, e);
        }
      default:
        LOG.error("Could not convert " + o + " of class " + o.getClass() + " into long.");
        throw new IOException("Could not convert " + o.getClass() + " into long");
        
    }
  }
  
  protected Text objToText(Object o, byte type) throws IOException {
    byte[] bytes = objToBytes(o, type);
    
    if (null == bytes) {
      LOG.warn("Creating empty text from null value");
      return new Text();
    }
    
    return new Text(bytes);
  }
  
  @SuppressWarnings("unchecked")
  protected byte[] objToBytes(Object o, byte type) throws IOException {
    if (o == null)
      return null;
    switch (type) {
      case DataType.BYTEARRAY:
        return ((DataByteArray) o).get();
      case DataType.BAG:
        return caster.toBytes((DataBag) o);
      case DataType.CHARARRAY:
        return caster.toBytes((String) o);
      case DataType.DOUBLE:
        return caster.toBytes((Double) o);
      case DataType.FLOAT:
        return caster.toBytes((Float) o);
      case DataType.INTEGER:
        return caster.toBytes((Integer) o);
      case DataType.LONG:
        return caster.toBytes((Long) o);
      case DataType.BIGINTEGER:
        return caster.toBytes((BigInteger) o);
      case DataType.BIGDECIMAL:
        return caster.toBytes((BigDecimal) o);
      case DataType.BOOLEAN:
        return caster.toBytes((Boolean) o);
      case DataType.DATETIME:
        return caster.toBytes((DateTime) o);
        
        // The type conversion here is unchecked.
        // Relying on DataType.findType to do the right thing.
      case DataType.MAP:
        return caster.toBytes((Map<String,Object>) o);
        
      case DataType.NULL:
        return null;
      case DataType.TUPLE:
        return caster.toBytes((Tuple) o);
      case DataType.ERROR:
        throw new IOException("Unable to determine type of " + o.getClass());
      default:
        throw new IOException("Unable to find a converter for tuple field " + o);
    }
  }
}
