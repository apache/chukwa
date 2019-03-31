package org.apache.hadoop.chukwa.pig;

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

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.record.Buffer;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.Expression;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataType;


public class ChukwaLoader extends LoadFunc implements LoadMetadata {
  SequenceFileRecordReader<ChukwaRecordKey, ChukwaRecord> reader;

  private TupleFactory tf = DefaultTupleFactory.getInstance();

  public ChukwaLoader() {
  }

  @Override
  public Tuple getNext() throws IOException {
    ChukwaRecord record = null;

    try {
      if (!reader.nextKeyValue()) {
        return null;
      }
    } catch (InterruptedException e) {
        throw new IOException(e);
    }

    record = reader.getCurrentValue();

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

  @Override
  public ResourceSchema getSchema(String s, Job job) throws IOException {
    Schema newSchema =  new Schema();
    newSchema.add(new Schema.FieldSchema("timestamp", DataType.LONG));
    newSchema.add(new Schema.FieldSchema("map", DataType.MAP));
    return new ResourceSchema(newSchema);
  }

  @Override
  public ResourceStatistics getStatistics(String s, Job job) throws IOException {
    return null;
  }

  @Override
  public String[] getPartitionKeys(String s, Job job) throws IOException {
    return null;
  }

  @Override
  public void setPartitionFilter(Expression expression) throws IOException {
  }

  @SuppressWarnings("unchecked")
  @Override
  public InputFormat getInputFormat() throws IOException {
    return new SequenceFileInputFormat<ChukwaRecordKey, ChukwaRecord>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
    this.reader = (SequenceFileRecordReader)reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }
}