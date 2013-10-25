package org.apache.accumulo.pig;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Maps;

public class AccumuloStorageTest {
  
  @Test
  public void test1Tuple() throws IOException {
    AccumuloStorage storage = new AccumuloStorage();
    
    Tuple t = TupleFactory.getInstance().newTuple(1);
    t.set(0, "row");
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(0, mutations.size());
  }
  
  @Test
  public void test2TupleNoColumn() throws IOException {
    AccumuloStorage storage = new AccumuloStorage();
    
    Tuple t = TupleFactory.getInstance().newTuple(2);
    t.set(0, "row");
    t.set(1, "value");
    
    Assert.assertEquals(0, storage.getMutations(t).size());
  }
  
  @Test
  public void test2TupleWithColumn() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col");
    
    Tuple t = TupleFactory.getInstance().newTuple(2);
    t.set(0, "row");
    t.set(1, "value");
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(1, colUpdates.size());
    
    ColumnUpdate colUpdate = colUpdates.get(0);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), new byte[0]));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "col".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value".getBytes()));
  }
  
  @Test
  public void test2TupleWithColumnQual() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col:qual");
    
    Tuple t = TupleFactory.getInstance().newTuple(2);
    t.set(0, "row");
    t.set(1, "value");
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(1, colUpdates.size());
    
    ColumnUpdate colUpdate = colUpdates.get(0);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), "col".getBytes()));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "qual".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value".getBytes()));
  }
  
  @Test
  public void test2TupleWithMixedColumns() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col1,col1:qual,col2:qual,col2");
    
    Tuple t = TupleFactory.getInstance().newTuple(5);
    t.set(0, "row");
    t.set(1, "value1");
    t.set(2, "value2");
    t.set(3, "value3");
    t.set(4, "value4");
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(4, colUpdates.size());
    
    ColumnUpdate colUpdate = colUpdates.get(0);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), new byte[0]));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "col1".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value1".getBytes()));
    
    colUpdate = colUpdates.get(1);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), "col1".getBytes()));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "qual".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value2".getBytes()));
    
    colUpdate = colUpdates.get(2);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), "col2".getBytes()));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "qual".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value3".getBytes()));
    
    colUpdate = colUpdates.get(3);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), new byte[0]));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "col2".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value4".getBytes()));
  }
  
  @Test
  public void testIgnoredExtraColumns() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col");
    
    Tuple t = TupleFactory.getInstance().newTuple(3);
    t.set(0, "row");
    t.set(1, "value1");
    t.set(2, "value2");
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(1, colUpdates.size());
    
    ColumnUpdate colUpdate = colUpdates.get(0);
    Assert.assertTrue("CF not equal", Arrays.equals(colUpdate.getColumnFamily(), new byte[0]));
    Assert.assertTrue("CQ not equal", Arrays.equals(colUpdate.getColumnQualifier(), "col".getBytes()));
    Assert.assertTrue("Values not equal", Arrays.equals(colUpdate.getValue(), "value1".getBytes()));
  }
  
  @Test
  public void testNonIgnoredExtraAsMap() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col");
    
    Map<String,Object> map = Maps.newHashMap();
    
    map.put("mapcol1", "mapval1");
    map.put("mapcol2", "mapval2");
    map.put("mapcol3", "mapval3");
    map.put("mapcol4", "mapval4");
    
    Tuple t = TupleFactory.getInstance().newTuple(3);
    t.set(0, "row");
    t.set(1, "value1");
    t.set(2, map);
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(5, colUpdates.size());
    
    Map<Entry<String,String>,String> expectations = Maps.newHashMap();
    expectations.put(Maps.immutableEntry("", "col"), "value1");
    expectations.put(Maps.immutableEntry("", "mapcol1"), "mapval1");
    expectations.put(Maps.immutableEntry("", "mapcol2"), "mapval2");
    expectations.put(Maps.immutableEntry("", "mapcol3"), "mapval3");
    expectations.put(Maps.immutableEntry("", "mapcol4"), "mapval4");
    
    for (ColumnUpdate update : colUpdates) {
      Entry<String,String> key = Maps.immutableEntry(new String(update.getColumnFamily()), new String(update.getColumnQualifier()));
      String value = new String(update.getValue());
      Assert.assertTrue(expectations.containsKey(key));
     
      String actual = expectations.remove(key);
      Assert.assertEquals(value, actual);
    }
    
    Assert.assertTrue("Did not find all expectations", expectations.isEmpty());
  }
  
  @Test
  public void testMapWithColFam() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col");
    
    Map<String,Object> map = Maps.newHashMap();
    
    map.put("mapcol1", "mapval1");
    map.put("mapcol2", "mapval2");
    map.put("mapcol3", "mapval3");
    map.put("mapcol4", "mapval4");
    
    Tuple t = TupleFactory.getInstance().newTuple(2);
    t.set(0, "row");
    t.set(1, map);
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(4, colUpdates.size());
    
    Map<Entry<String,String>,String> expectations = Maps.newHashMap();
    expectations.put(Maps.immutableEntry("col", "mapcol1"), "mapval1");
    expectations.put(Maps.immutableEntry("col", "mapcol2"), "mapval2");
    expectations.put(Maps.immutableEntry("col", "mapcol3"), "mapval3");
    expectations.put(Maps.immutableEntry("col", "mapcol4"), "mapval4");
    
    for (ColumnUpdate update : colUpdates) {
      Entry<String,String> key = Maps.immutableEntry(new String(update.getColumnFamily()), new String(update.getColumnQualifier()));
      String value = new String(update.getValue());
      Assert.assertTrue(expectations.containsKey(key));
     
      String actual = expectations.remove(key);
      Assert.assertEquals(value, actual);
    }
    
    Assert.assertTrue("Did not find all expectations", expectations.isEmpty());
  }
  
  @Test
  public void testMapWithColFamColQualPrefix() throws IOException {
    AccumuloStorage storage = new AccumuloStorage("col:qual_");
    
    Map<String,Object> map = Maps.newHashMap();
    
    map.put("mapcol1", "mapval1");
    map.put("mapcol2", "mapval2");
    map.put("mapcol3", "mapval3");
    map.put("mapcol4", "mapval4");
    
    Tuple t = TupleFactory.getInstance().newTuple(2);
    t.set(0, "row");
    t.set(1, map);
    
    Collection<Mutation> mutations = storage.getMutations(t);
    
    Assert.assertEquals(1, mutations.size());
    
    Mutation m = mutations.iterator().next();
    
    Assert.assertTrue("Rows not equal", Arrays.equals(m.getRow(), ((String) t.get(0)).getBytes()));
    
    List<ColumnUpdate> colUpdates = m.getUpdates();
    Assert.assertEquals(4, colUpdates.size());
    
    Map<Entry<String,String>,String> expectations = Maps.newHashMap();
    expectations.put(Maps.immutableEntry("col", "qual_mapcol1"), "mapval1");
    expectations.put(Maps.immutableEntry("col", "qual_mapcol2"), "mapval2");
    expectations.put(Maps.immutableEntry("col", "qual_mapcol3"), "mapval3");
    expectations.put(Maps.immutableEntry("col", "qual_mapcol4"), "mapval4");
    
    for (ColumnUpdate update : colUpdates) {
      Entry<String,String> key = Maps.immutableEntry(new String(update.getColumnFamily()), new String(update.getColumnQualifier()));
      String value = new String(update.getValue());
      Assert.assertTrue(expectations.containsKey(key));
     
      String actual = expectations.remove(key);
      Assert.assertEquals(value, actual);
    }
    
    Assert.assertTrue("Did not find all expectations", expectations.isEmpty());
  }
  
}
