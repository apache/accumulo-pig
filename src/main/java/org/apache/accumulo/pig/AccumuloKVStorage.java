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
import java.util.Collection;
import java.util.Collections;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

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
  
}
