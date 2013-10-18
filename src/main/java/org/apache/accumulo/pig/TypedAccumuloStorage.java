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
import java.util.Properties;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.LoadPushDown.RequiredFieldList;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
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
public class TypedAccumuloStorage extends AbstractAccumuloStorage {
  private static final Log LOG = LogFactory.getLog(TypedAccumuloStorage.class);
  protected LoadStoreCaster caster;
  protected String contextSignature = null;
  
  private ResourceSchema schema_;
  private RequiredFieldList requiredFieldList;
  
  public TypedAccumuloStorage() {
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
    ResourceFieldSchema[] fieldSchemas = (schema_ == null) ? null : schema_.getFields();
    
    Text t = tupleToText(tuple, 0, fieldSchemas);
    
    Mutation mut = new Mutation(t);
    Text cf = tupleToText(tuple, 1, fieldSchemas);
    Text cq = tupleToText(tuple, 2, fieldSchemas);
    
    if (tuple.size() > 4) {
      Text cv = tupleToText(tuple, 3, fieldSchemas);
      
      byte[] valueBytes = tupleToBytes(tuple, 4, fieldSchemas);
      
      Value val = new Value(valueBytes);
      if (cv.getLength() == 0) {
        mut.put(cf, cq, val);
      } else {
        mut.put(cf, cq, new ColumnVisibility(cv), val);
      }
    } else {
      byte[] valueBytes = tupleToBytes(tuple, 3, fieldSchemas);
      Value val = new Value(valueBytes);
      mut.put(cf, cq, val);
    }
    
    return Collections.singleton(mut);
  }
  
  @Override
  public void setUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }
  
  @Override
  public void setStoreFuncUDFContextSignature(String signature) {
    this.contextSignature = signature;
  }
  
  /**
   * Returns UDFProperties based on <code>contextSignature</code>.
   */
  private Properties getUDFProperties() {
    return UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[] {contextSignature});
  }
  
  @Override
  public void checkSchema(ResourceSchema s) throws IOException {
    if (!(caster instanceof LoadStoreCaster)) {
      LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
      throw new IOException("Bad Caster " + caster.getClass());
    }
    schema_ = s;
    getUDFProperties().setProperty(contextSignature + "_schema", ObjectSerializer.serialize(schema_));
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
