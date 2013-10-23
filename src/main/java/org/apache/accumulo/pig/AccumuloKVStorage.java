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
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.ObjectSerializer;
import org.joda.time.DateTime;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Accumulo.
 * 
 * A Key/Val pair will be returned as tuples: (key, colfam, colqual, colvis, timestamp, value). All fields except timestamp are DataByteArray, timestamp is a
 * long.
 * 
 * <p>Tuples require at least key, column family, column qualifier and value; however column visibility or column visibility and timestamp may also be
 * provided:</p>
 * 
 * <ul>
 * <li>(key, colfam, colqual, value)</li>
 * <li>(key, colfam, colqual, colvis, value)</li>
 * <li>(key, colfam, colqual, colvis, timestamp, value)</li> 
 * </ul>
 */
public class AccumuloKVStorage extends AbstractAccumuloStorage {
  private static final Log LOG = LogFactory.getLog(AccumuloKVStorage.class);
  protected LoadStoreCaster caster;
  
  private ResourceSchema schema;
  
  public AccumuloKVStorage() {
    this.caster = new Utf8StorageConverter();
  }
  
  @Override
  protected Tuple getTuple(Key key, Value value) throws IOException {
    // and wrap it in a tuple
    Tuple tuple = TupleFactory.getInstance().newTuple(6);
    tuple.set(0, new DataByteArray(key.getRow().getBytes()));
    tuple.set(1, new DataByteArray(key.getColumnFamily().getBytes()));
    tuple.set(2, new DataByteArray(key.getColumnQualifier().getBytes()));
    tuple.set(3, new DataByteArray(key.getColumnVisibility().getBytes()));
    tuple.set(4, new Long(key.getTimestamp()));
    tuple.set(5, new DataByteArray(value.get()));
    return tuple;
  }
  
  @Override
  public Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException {
    ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();
    
    Text t = tupleToText(tuple, 0, fieldSchemas);
    
    Mutation mut = new Mutation(t);
    Text cf = tupleToText(tuple, 1, fieldSchemas);
    Text cq = tupleToText(tuple, 2, fieldSchemas);
    
    if (4 == tuple.size()) {
      byte[] valueBytes = tupleToBytes(tuple, 3, fieldSchemas);
      Value val = new Value(valueBytes);
      
      mut.put(cf, cq, val);
    } else if (5 == tuple.size()) {
      Text cv = tupleToText(tuple, 3, fieldSchemas);
      
      byte[] valueBytes = tupleToBytes(tuple, 4, fieldSchemas);
      
      Value val = new Value(valueBytes);
      if (cv.getLength() == 0) {
        mut.put(cf, cq, val);
      } else {
        mut.put(cf, cq, new ColumnVisibility(cv), val);
      }
    } else {
      if (6 < tuple.size()) {
        LOG.debug("Ignoring additional entries in tuple of length " + tuple.size());
      }
      
      Text cv = tupleToText(tuple, 3, fieldSchemas);
      
      long ts = objToLong(tuple, 4, fieldSchemas);
      
      byte[] valueBytes = tupleToBytes(tuple, 5, fieldSchemas);
      
      Value val = new Value(valueBytes);
      if (cv.getLength() == 0) {
        mut.put(cf, cq, val);
      } else {
        mut.put(cf, cq, new ColumnVisibility(cv), ts, val);
      }
    }
    
    return Collections.singleton(mut);
  }
  
  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    if (!(caster instanceof LoadStoreCaster)) {
      LOG.error("Caster must implement LoadStoreCaster for writing to Accumulo.");
      throw new IOException("Bad Caster " + caster.getClass());
    }
    schema = s;
    getUDFProperties().setProperty(contextSignature + "_schema", ObjectSerializer.serialize(schema));
  }
  
  private Text tupleToText(Tuple tuple, int i, ResourceFieldSchema[] fieldSchemas) throws IOException {
    Object o = tuple.get(i);
    byte type = schemaToType(o, i, fieldSchemas);
    
    return objToText(o, type);
  }
  
  private byte schemaToType(Object o, int i, ResourceFieldSchema[] fieldSchemas) {
    return (fieldSchemas == null) ? DataType.findType(o) : fieldSchemas[i].getType();
  }
  
  private byte[] tupleToBytes(Tuple tuple, int i, ResourceFieldSchema[] fieldSchemas) throws IOException {
    Object o = tuple.get(i);
    byte type = schemaToType(o, i, fieldSchemas);
    
    return objToBytes(o, type);
    
  }
  
  private long objToLong(Tuple tuple, int i, ResourceFieldSchema[] fieldSchemas) throws IOException { 
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
  
  private Text objToText(Object o, byte type) throws IOException {
    return new Text(objToBytes(o, type));
  }
  
  @SuppressWarnings("unchecked")
  private byte[] objToBytes(Object o, byte type) throws IOException {
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
