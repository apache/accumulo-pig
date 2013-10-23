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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

public class AccumuloKVStorageTest {
  
  @Test
  public void testGetMutations4() throws Exception {
    AccumuloKVStorage s = new AccumuloKVStorage();
    
    Tuple tuple = TupleFactory.getInstance().newTuple(4);
    tuple.set(0, "row1");
    tuple.set(1, "cf1");
    tuple.set(2, "cq1");
    tuple.set(3, "val1");
    
    Collection<Mutation> muts = s.getMutations(tuple);
    
    assertNotNull(muts);
    assertEquals(1, muts.size());
    Mutation mut = muts.iterator().next();
    List<ColumnUpdate> updates = mut.getUpdates();
    assertEquals(1, updates.size());
    ColumnUpdate update = updates.get(0);
    
    assertTrue(Arrays.equals(((String) tuple.get(0)).getBytes(), mut.getRow()));
    assertTrue(Arrays.equals(((String) tuple.get(1)).getBytes(), update.getColumnFamily()));
    assertTrue(Arrays.equals(((String) tuple.get(2)).getBytes(), update.getColumnQualifier()));
    assertTrue(Arrays.equals(((String) tuple.get(3)).getBytes(), update.getValue()));
    assertTrue(Arrays.equals("".getBytes(), update.getColumnVisibility()));
  }
  
  @Test
  public void testGetMutations5() throws Exception {
    AccumuloKVStorage s = new AccumuloKVStorage();
    
    Tuple tuple = TupleFactory.getInstance().newTuple(5);
    tuple.set(0, "row1");
    tuple.set(1, "cf1");
    tuple.set(2, "cq1");
    tuple.set(3, "cv1");
    tuple.set(4, "val1");
    
    Collection<Mutation> muts = s.getMutations(tuple);
    
    assertNotNull(muts);
    assertEquals(1, muts.size());
    Mutation mut = muts.iterator().next();
    List<ColumnUpdate> updates = mut.getUpdates();
    assertEquals(1, updates.size());
    ColumnUpdate update = updates.get(0);
    
    assertTrue(Arrays.equals(((String) tuple.get(0)).getBytes(), mut.getRow()));
    assertTrue(Arrays.equals(((String) tuple.get(1)).getBytes(), update.getColumnFamily()));
    assertTrue(Arrays.equals(((String) tuple.get(2)).getBytes(), update.getColumnQualifier()));
    assertTrue(Arrays.equals(((String) tuple.get(3)).getBytes(), update.getColumnVisibility()));
    assertTrue(Arrays.equals(((String) tuple.get(4)).getBytes(), update.getValue()));
  }
  
  @Test
  public void testGetMutations6() throws Exception {
    AccumuloKVStorage s = new AccumuloKVStorage();
    
    Tuple tuple = TupleFactory.getInstance().newTuple(6);
    tuple.set(0, "row");
    tuple.set(1, "cf");
    tuple.set(2, "cq");
    tuple.set(3, "cv");
    tuple.set(4, new Long(1));
    tuple.set(5, "value");
    
    Collection<Mutation> mutations = s.getMutations(tuple);
    Assert.assertNotNull(mutations);
    Assert.assertEquals(1, mutations.size());
    Mutation m = mutations.iterator().next();
    
    List<ColumnUpdate> updates = m.getUpdates();
    Assert.assertEquals(1, updates.size());
    ColumnUpdate update = updates.get(0);
    
    assertTrue(Arrays.equals(((String) tuple.get(0)).getBytes(), m.getRow()));
    assertTrue(Arrays.equals(((String) tuple.get(1)).getBytes(), update.getColumnFamily()));
    assertTrue(Arrays.equals(((String) tuple.get(2)).getBytes(), update.getColumnQualifier()));
    assertTrue(Arrays.equals(((String) tuple.get(3)).getBytes(), update.getColumnVisibility()));
    assertEquals(((Long) tuple.get(4)).longValue(), update.getTimestamp());
    assertTrue(Arrays.equals(((String) tuple.get(5)).getBytes(), update.getValue()));
  }
  
  @Test
  public void testGetTuple() throws Exception {
    AccumuloKVStorage s = new AccumuloKVStorage();
    
    Key key = new Key("row1", "cf1", "cq1", "cv1", 1024L);
    Value value = new Value("val1".getBytes());
    Tuple tuple = s.getTuple(key, value);
    TestUtils.assertKeyValueEqualsTuple(key, value, tuple);
    
    key = new Key("row1", "cf1", "cq1");
    value = new Value("val1".getBytes());
    tuple = s.getTuple(key, value);
    TestUtils.assertKeyValueEqualsTuple(key, value, tuple);
  }
}
