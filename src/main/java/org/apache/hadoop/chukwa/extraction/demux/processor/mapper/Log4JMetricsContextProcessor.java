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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.Iterator;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.JSONObject;

public class Log4JMetricsContextProcessor extends AbstractProcessor {

  static Logger log = Logger.getLogger(Log4JMetricsContextProcessor.class);

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable 
  {
    Log4JMetricsContextChukwaRecord record = new Log4JMetricsContextChukwaRecord(recordEntry);
    ChukwaRecord chukwaRecord = record.getChukwaRecord();
    this.buildGenericRecord(chukwaRecord, null, record.getTimestamp(), record.getRecordType());
    output.collect(key, chukwaRecord);
  }

  // create a static class to cove most of the code for unit test 
  static class Log4JMetricsContextChukwaRecord {
    private String recordType = null;
    private long timestamp = 0;
    private ChukwaRecord chukwaRecord = new ChukwaRecord();
    
    @SuppressWarnings("unchecked")
    public Log4JMetricsContextChukwaRecord(String recordEntry) throws Throwable {
      LogEntry log = new LogEntry(recordEntry);
      JSONObject json = new JSONObject(log.getBody());

      // round timestamp
      timestamp = json.getLong("chukwa_timestamp");
      timestamp = (timestamp / 60000) * 60000;

      // get record type
      String contextName = json.getString("contextName");
      String recordName = json.getString("recordName");
      recordType = contextName;
      if (!contextName.equals(recordName)) {
        recordType += "_" + recordName;
      }

      Iterator<String> ki = json.keys();
      while (ki.hasNext()) {
        String key = ki.next();
        String value = json.getString(key);
        if(value != null) {
          chukwaRecord.add(key, value);
        }
      }
    }

    public String getRecordType() {
      return recordType;
    }

    public long getTimestamp() {
      return timestamp;
    }
    
    public ChukwaRecord getChukwaRecord() {
      return chukwaRecord;
    }
  }
}

