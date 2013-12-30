package org.apache.accumulo.pig;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AccumuloStorageConfigurationTest {

  protected Configuration original;
  protected AccumuloStorage storage;
  
  @Before
  public void setup() {
    storage = new AccumuloStorage();
    
    original = new Configuration();
    
    original.set("string1", "value1");
    original.set("string2", "value2");
    original.set("string3", "value3");
    original.setBoolean("boolean", true);
    original.setLong("long", 10);
    original.setInt("integer", 20);
  }
  
  protected Map<String,String> getContents(Configuration conf) {
    Map<String,String> contents = new HashMap<String,String>();
    Iterator<Entry<String,String>> iter = conf.iterator();
    while (iter.hasNext()) {
      Entry<String,String> entry = iter.next();
      
      contents.put(entry.getKey(), entry.getValue());
    }
    
    return contents;
  }
  
  
  @Test
  public void testEquivalence() {
    Configuration unsetCopy = new Configuration(original), clearCopy = new Configuration(original);
    
    Assert.assertEquals(getContents(unsetCopy), getContents(clearCopy));
    
    Map<String,String>  entriesToUnset = new HashMap<String,String>();
    entriesToUnset.put("string1", "foo");
    entriesToUnset.put("string3", "bar");
    
    storage.simpleUnset(unsetCopy, entriesToUnset);
    storage.replaceUnset(clearCopy, entriesToUnset);
    
    Assert.assertEquals(getContents(unsetCopy), getContents(clearCopy));
    
    Configuration originalCopy = new Configuration(original);
    originalCopy.unset("string1");
    originalCopy.unset("string3");
    
    Assert.assertEquals(getContents(originalCopy), getContents(unsetCopy));
    Assert.assertEquals(getContents(originalCopy), getContents(clearCopy));
  }
  
  
  @Test
  public void testEquivalenceOnTypes() {
    Configuration unsetCopy = new Configuration(original), clearCopy = new Configuration(original);
    
    Assert.assertEquals(getContents(unsetCopy), getContents(clearCopy));
    
    Map<String,String>  entriesToUnset = new HashMap<String,String>();
    entriesToUnset.put("long", "foo");
    entriesToUnset.put("boolean", "bar");
    entriesToUnset.put("integer", "foobar");
    
    storage.simpleUnset(unsetCopy, entriesToUnset);
    storage.replaceUnset(clearCopy, entriesToUnset);
    
    Assert.assertEquals(getContents(unsetCopy), getContents(clearCopy));
    
    Configuration originalCopy = new Configuration(original);
    originalCopy.unset("long");
    originalCopy.unset("boolean");
    originalCopy.unset("integer");
    
    Assert.assertEquals(getContents(originalCopy), getContents(unsetCopy));
    Assert.assertEquals(getContents(originalCopy), getContents(clearCopy));
  }

}
