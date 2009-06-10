/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.chukwa;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileRecordReader;
import org.apache.hadoop.record.Buffer;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.logicalLayer.schema.Schema;


public class ChukwaStorage extends Utf8StorageConverter implements LoadFunc,
    StoreFunc
{
  Schema schema = null;
  SequenceFileRecordReader<ChukwaRecordKey, ChukwaRecord> reader;
  SequenceFile.Reader r;
  SequenceFile.Writer writer;
  DataOutputStream dos;
  FSDataOutputStream fsd = null;
  Calendar calendar = Calendar.getInstance();

  int timestampFieldIndex = -1;
  int pkFieldIndex = -1;
  int sourceFieldIndex = -1;
  int clusterNameFieldIndex = -1;
  int recordTypeFieldIndex = -1;
  int applicationFieldIndex = -1;
  
  String[] fields = null;
  private TupleFactory tf = DefaultTupleFactory.getInstance();

  public ChukwaStorage() {
  }
  
  public ChukwaStorage(String... scfields ) {

    this.fields = scfields;
    for (int i=0;i< scfields.length;i++) {
      if (scfields[i].equalsIgnoreCase("c_timestamp")) {
        timestampFieldIndex = i;
      } else if (scfields[i].equalsIgnoreCase("c_pk")) {
        pkFieldIndex = i;
      } else if (scfields[i].equalsIgnoreCase("c_source")) {
        sourceFieldIndex = i;
      } else if (scfields[i].equalsIgnoreCase("c_recordtype")) {
        recordTypeFieldIndex =i;
      } else if (scfields[i].equalsIgnoreCase("c_application")) {
        applicationFieldIndex =i;
      } else if (scfields[i].equalsIgnoreCase("c_cluster")) {
        clusterNameFieldIndex =i;
      } 
  }

  }
  public void bindTo(String fileName, BufferedPositionedInputStream is,
      long offset, long end) throws IOException
  {
    JobConf conf = PigInputFormat.sJob;
    if (conf == null) {
      conf = new JobConf();
    }
    
    FileSplit split = new FileSplit(new Path(fileName), offset, end - offset,
        (String[]) null);
    reader = new SequenceFileRecordReader<ChukwaRecordKey, ChukwaRecord>(conf,
        split);
    if (reader.getValueClass() != ChukwaRecord.class)
      throw new IOException(
          "The value class in the sequence file does not match that for Chukwa data");

  }

  public Tuple getNext() throws IOException
  {
    ChukwaRecord record = new ChukwaRecord();
    if (!reader.next(reader.createKey(), record))
      return null;

    Tuple ret = tf.newTuple(2);
    try
    {
      ret.set(0, new Long(record.getTime()));

      HashMap<Object, Object> pigMapFields = new HashMap<Object, Object>();
      TreeMap<String, Buffer> mapFields = record.getMapFields();

      if (mapFields != null)
      {
        for (String key : mapFields.keySet())
        {
          pigMapFields.put(key, new DataByteArray(record.getValue(key).getBytes()));
        }
      }
      ret.set(1, pigMapFields);

    } catch (ExecException e)
    {
      e.printStackTrace();
      throw new IOException(e);
    }
    return ret;
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.pig.LoadFunc#fieldsToRead(org.apache.pig.impl.logicalLayer.schema.Schema)
   */
  public void fieldsToRead(Schema schema)
  {
  }

  public Schema determineSchema(String fileName, ExecType execType,
      DataStorage storage) throws IOException
  {
    Schema newSchema =  new Schema();
    newSchema.add(new Schema.FieldSchema("timestamp", DataType.LONG));
    newSchema.add(new Schema.FieldSchema("map", DataType.MAP));
   
    return schema;
  }

  @SuppressWarnings("deprecation")
  @Override
  public void bindTo(OutputStream os) throws IOException
  {
    JobConf conf = new JobConf();
    dos = new DataOutputStream(os);
    fsd = new FSDataOutputStream(dos);
    writer = SequenceFile.createWriter(conf, fsd,
        ChukwaRecordKey.class, ChukwaRecord.class,
        SequenceFile.CompressionType.BLOCK, new DefaultCodec());
  }

  @Override
  public void finish() throws IOException
  {
    if (reader != null) {
      try {
        reader.close();
      }catch(Throwable e) {
      }
    }
    
    if (writer != null) {
      try {
        writer.close();
      }catch(Throwable e) {
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void putNext(Tuple f) throws IOException
  {
    long timePartition = 0l;
    long timestamp = 0L;
    String source = "N/A";
    String application = "N/A";
    String recordType = "N/A";
    String clusterName = "N/A";
    String pk = "";
    
    try
    {

      ChukwaRecordKey key = new ChukwaRecordKey();
     

      ChukwaRecord record = new ChukwaRecord();
      record.setTime(System.currentTimeMillis());
      int inputSize = f.size();      
      for(int i=0;i<inputSize;i++) {
        Object field = f.get(i);
        
        if (field == null) {
          continue;
        }
        
        if (i == this.pkFieldIndex) {
          pk = field.toString();
          continue;
        } else if ( i == this.sourceFieldIndex) {
          source = field.toString();
          continue;
          
        }else if ( i== this.recordTypeFieldIndex) {
          recordType = field.toString();
          continue;
          
        }else if ( i== this.applicationFieldIndex) {
          application = field.toString();
          continue;
          
        } else if ( i== this.clusterNameFieldIndex) {
          clusterName = field.toString();
          continue;
          
        }else if (i == this.timestampFieldIndex) {
          
          timestamp = Long.parseLong(field.toString());
          record.setTime(timestamp);

          synchronized (calendar)
          {
            calendar.setTimeInMillis(timestamp);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
            calendar.set(Calendar.MILLISECOND, 0);
            timePartition = calendar.getTimeInMillis();
          }
          record.setTime(Long.parseLong(field.toString()));
          continue;
          
        }  else if (field instanceof Map) {
          Map<Object, Object> m = (Map<Object, Object>)field;
          for(Object o: m.keySet()) {
            record.add(o.toString(),m.get(o).toString());
          }
          continue;
        } else {
          if (i <fields.length ) {
            record.add(fields[i],field.toString());
          } else {
            record.add("field-"+i,field.toString());
          }
          
          continue;
        }
      }

      record.add(Record.tagsField, " cluster=\"" + clusterName.trim() + "\" ");
      record.add(Record.sourceField, source);
      record.add(Record.applicationField, application);
      key.setKey("" + timePartition + "/" + pk + "/" + timestamp);
      key.setReduceType(recordType);
      
      writer.append(key, record);
    } catch (ExecException e)
    {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    }
  }

  @SuppressWarnings("unchecked")
  public Class getStorePreparationClass() throws IOException {
    return null;
  }

}
