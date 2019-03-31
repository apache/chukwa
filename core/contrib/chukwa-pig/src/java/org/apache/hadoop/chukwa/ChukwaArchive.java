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

import java.io.IOException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadMetadata;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.Expression;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultTupleFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import static org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ChukwaArchive extends LoadFunc implements LoadMetadata {

  SequenceFileRecordReader<ChukwaArchiveKey, ChunkImpl> reader;

  private TupleFactory tf = DefaultTupleFactory.getInstance();
  
  static Schema chukwaArchiveSchema;
  static ResourceSchema chukwaArchiveResourceSchema;
  static int schemaFieldCount;
  static {
    chukwaArchiveSchema = new Schema();
    chukwaArchiveSchema.add(new FieldSchema("seqNo", DataType.LONG));
    chukwaArchiveSchema.add(new FieldSchema("type", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("name", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("source", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("tags", DataType.CHARARRAY));
    chukwaArchiveSchema.add(new FieldSchema("data", DataType.BYTEARRAY));
    schemaFieldCount = chukwaArchiveSchema.size();
    chukwaArchiveResourceSchema = new ResourceSchema(chukwaArchiveSchema);
    //do we want to expose the record offsets?
  }

  @Override
  public Tuple getNext() throws IOException {
    
    try {
      if (!reader.nextKeyValue()) {
        return null;
      }
    } catch (InterruptedException e) {
        throw new IOException(e);
    }

    ChunkImpl val = ChunkImpl.getBlankChunk();
    Tuple t = tf.newTuple(schemaFieldCount);
    t.set(0, new Long(val.seqID));
    t.set(1, val.getDataType());
    t.set(2, val.getStreamName());
    t.set(3, val.getSource());
    t.set(4, val.getTags());
    byte[] data = val.getData();
  
    t.set(5, (data == null) ? new DataByteArray() : new DataByteArray(data));
    
//    System.out.println("returning " + t);
    return t;
  }

  @Override
  public ResourceSchema getSchema(String s, Job job) throws IOException {
    return chukwaArchiveResourceSchema;
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
    return new SequenceFileInputFormat<ChukwaArchiveKey, ChunkImpl>();
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
