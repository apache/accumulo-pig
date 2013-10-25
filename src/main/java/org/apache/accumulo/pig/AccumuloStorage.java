package org.apache.accumulo.pig;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.InternalMap;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.collect.Lists;

public class AccumuloStorage extends AbstractAccumuloStorage {
  private static final Logger log = Logger.getLogger(AccumuloStorage.class);
  private static final String COMMA = ",", COLON = ":";
  private static final Text EMPTY_TEXT = new Text(new byte[0]);
  
  protected final List<String> columnSpecs;
  
  public AccumuloStorage() {
    this("");
  }
  
  public AccumuloStorage(String columns) {
    this.caster = new Utf8StorageConverter();
    
    if (!StringUtils.isBlank(columns)) {
      String[] columnArray = StringUtils.split(columns, COMMA);
      columnSpecs = Lists.newArrayList(columnArray);
    } else {
      columnSpecs = Collections.emptyList();
    }
  }
  
  @Override
  protected Tuple getTuple(Key key, Value value) throws IOException {
    
    SortedMap<Key,Value> rowKVs = WholeRowIterator.decodeRow(key, value);
    
    List<Object> tupleEntries = Lists.newLinkedList();
    Iterator<Entry<Key,Value>> iter = rowKVs.entrySet().iterator();
    List<Entry<Key,Value>> aggregate = Lists.newLinkedList();
    Entry<Key,Value> currentEntry = null;
    
    while (iter.hasNext()) {
      if (null == currentEntry) {
        currentEntry = iter.next();
        aggregate.add(currentEntry);
      } else {
        Entry<Key,Value> nextEntry = iter.next();
        
        // If we have the same colfam
        if (currentEntry.getKey().equals(nextEntry.getKey(), PartialKey.ROW_COLFAM)) {
          // Aggregate this entry into the map
          aggregate.add(nextEntry);
        } else {
          currentEntry = nextEntry;
          
          // Flush and start again
          InternalMap map = aggregate(aggregate);
          tupleEntries.add(map);
          
          aggregate = Lists.newLinkedList();
          aggregate.add(currentEntry);
        }
      }
    }
    
    if (!aggregate.isEmpty()) {
      tupleEntries.add(aggregate(aggregate));
    }
    
    // and wrap it in a tuple
    Tuple tuple = TupleFactory.getInstance().newTuple(tupleEntries.size() + 1);
    tuple.set(0, new DataByteArray(key.getRow().getBytes()));
    int i = 1;
    for (Object obj : tupleEntries) {
      tuple.set(i, obj);
      i++;
    }
    
    return tuple;
  }
  
  private InternalMap aggregate(List<Entry<Key,Value>> columns) {
    InternalMap map = new InternalMap();
    for (Entry<Key,Value> column : columns) {
      map.put(column.getKey().getColumnFamily().toString() + COLON + column.getKey().getColumnQualifier().toString(),
          new DataByteArray(column.getValue().get()));
    }
    
    return map;
  }
  
  protected void configureInputFormat(Configuration conf) {
    AccumuloInputFormat.addIterator(conf, new IteratorSetting(50, WholeRowIterator.class));
  }
  
  @Override
  public Collection<Mutation> getMutations(Tuple tuple) throws ExecException, IOException {
    final ResourceFieldSchema[] fieldSchemas = (schema == null) ? null : schema.getFields();
    
    Iterator<Object> tupleIter = tuple.iterator();
    
    if (1 >= tuple.size()) {
      log.debug("Ignoring tuple of size " + tuple.size());
      return Collections.emptyList();
    }
    
    Mutation mutation = new Mutation(objectToText(tupleIter.next(), (null == fieldSchemas) ? null : fieldSchemas[0]));
    
    // TODO Can these be lifted up to members of the class instead of this method?
    // Not sure if AccumuloStorage instances need to be thread-safe or not
    final Text _cfHolder = new Text(), _cqHolder = new Text();
    
    int columnOffset = 0;
    int tupleOffset = 1;
    while (tupleIter.hasNext()) {
      Object o = tupleIter.next();
      String cf = null;
      
      // Figure out if the user provided a specific columnfamily to use.
      if (columnOffset < columnSpecs.size()) {
        cf = columnSpecs.get(columnOffset);
      }
      
      // Grab the type for this field
      byte type = schemaToType(o, (null == fieldSchemas) ? null : fieldSchemas[tupleOffset]);
      
      // If we have a Map, we want to treat every Entry as a column in this record
      // placing said column in the column family unless this instance of AccumuloStorage
      // was provided a specific columnFamily to use, in which case the entry's column is
      // in the column qualifier.
      if (DataType.MAP == type) {
        @SuppressWarnings("unchecked")
        Map<String,Object> map = (Map<String,Object>) o;
        
        for (Entry<String,Object> entry : map.entrySet()) {
          Object entryObject = entry.getValue();
          byte entryType = DataType.findType(entryObject);
          
          Value value = new Value(objToBytes(entryObject, entryType));
          
          // If we have a CF, use it and push the Map's key down to the CQ
          if (null != cf) {
            int index = cf.indexOf(COLON);
            
            // No colon in the provided column
            if (-1 == index) {
              _cfHolder.set(cf);
              _cqHolder.set(entry.getKey());
              
              mutation.put(_cfHolder, _cqHolder, value);
            } else {
              _cfHolder.set(cf.getBytes(), 0, index);
              
              _cqHolder.set(cf.getBytes(), index + 1, cf.length() - (index + 1));
              _cqHolder.append(entry.getKey().getBytes(), 0, entry.getKey().length());
              
              mutation.put(_cfHolder, _cqHolder, value);
            }
          } else {
            // Just put the Map's key into the CQ
            _cqHolder.set(entry.getKey());
            mutation.put(EMPTY_TEXT, _cqHolder, value);
          }
        }
      } else if (null == cf) {
        // We don't know what column to place the value into
        log.warn("Was provided no column family for non-Map entry in the tuple at offset " + tupleOffset);
      } else {
        Value value = new Value(objToBytes(o, type));
        
        // We have something that isn't a Map, use the provided CF as a column name
        // and then shove the value into the Value
        int index = cf.indexOf(COLON);
        if (-1 == index) {
          _cqHolder.set(cf);
          
          mutation.put(EMPTY_TEXT, _cqHolder, value);
        } else {
          byte[] cfBytes = cf.getBytes();
          _cfHolder.set(cfBytes, 0, index);
          _cqHolder.set(cfBytes, index + 1, cfBytes.length - (index + 1));
          
          mutation.put(_cfHolder, _cqHolder, value);
        }
      }
      
      columnOffset++;
      tupleOffset++;
    }
    
    if (0 == mutation.size()) {
      return Collections.emptyList();
    }
    
    return Collections.singletonList(mutation);
  }
}
