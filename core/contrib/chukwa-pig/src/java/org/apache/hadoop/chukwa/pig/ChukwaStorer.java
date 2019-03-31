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
import java.util.Calendar;
import java.util.Map;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;


public class ChukwaStorer extends StoreFunc {
  RecordWriter<ChukwaRecordKey, ChukwaRecord> writer;
  Calendar calendar = Calendar.getInstance();

  int timestampFieldIndex = -1;
  int pkFieldIndex = -1;
  int sourceFieldIndex = -1;
  int clusterNameFieldIndex = -1;
  int recordTypeFieldIndex = -1;
  int applicationFieldIndex = -1;

  String[] fields = null;

  public ChukwaStorer() {
  }

  public ChukwaStorer(String... scfields ) {

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

  @Override
  public void putNext(Tuple f) throws IOException {

    long timePartition = 0l;
    long timestamp = 0L;
    String source = "N/A";
    String application = "N/A";
    String recordType = "N/A";
    String clusterName = "N/A";
    String pk = "";

    try {

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

      writer.write(key, record);
    } catch (ExecException e) {
      IOException ioe = new IOException();
      ioe.initCause(e);
      throw ioe;
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new SequenceFileOutputFormat<ChukwaRecordKey, ChukwaRecord>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    FileOutputFormat.setOutputPath(job, new Path(location));
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
    job.setOutputKeyClass(ChukwaRecordKey.class);
    job.setOutputValueClass(ChukwaRecord.class);
  }
}