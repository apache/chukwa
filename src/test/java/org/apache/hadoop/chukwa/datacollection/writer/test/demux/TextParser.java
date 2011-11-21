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

package org.apache.hadoop.chukwa.datacollection.writer.test.demux;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.AbstractProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

@Table(name="Test",columnFamily="TestColumnFamily")
public class TextParser extends AbstractProcessor {
  static Logger log = Logger.getLogger(TextParser.class);
  public static final String reduceType = "TestColumnFamily";
  public final String recordType = this.getClass().getName();

  public TextParser() {
  }

  public String getDataType() {
    return recordType;
  }

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    ChukwaRecord record = new ChukwaRecord();
    String[] parts = recordEntry.split("\\s");
    record.add("timestamp", parts[0]);
    record.add(parts[1], parts[2]);
    key.setKey(parts[0]+"/"+parts[1]+"/"+parts[0]);
    long timestamp = Long.parseLong(parts[0]);
    this.buildGenericRecord(record, null, timestamp, reduceType);
    output.collect(key, record);    
  }
}