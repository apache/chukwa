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


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

@Tables(annotations={
@Table(name="Jobs",columnFamily="summary")
})
public class JobSummary extends AbstractProcessor {  
  static Logger log = Logger.getLogger(JobSummary.class);
  static final String chukwaTimestampField = "chukwa_timestamp";
  static final String contextNameField = "contextName";
  static final String recordNameField = "recordName";

  private SimpleDateFormat sdf = null;

  public JobSummary() {
    // TODO move that to config
    sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    try {
      // Look for syslog PRI, if PRI is not found, start from offset of 0.
      int idx = recordEntry.indexOf('>', 0);  
      String dStr = recordEntry.substring(idx+1, idx+23);
      int start = idx + 25;
      idx = recordEntry.indexOf(' ', start);
      // String level = recordEntry.substring(start, idx);
      start = idx + 1;
      idx = recordEntry.indexOf(' ', start);
      // String className = recordEntry.substring(start, idx-1);
      String body = recordEntry.substring(idx + 1);
      body.replaceAll("\n", "");
      // log.info("record [" + recordEntry + "] body [" + body +"]");
      Date d = sdf.parse(dStr);

      ChukwaRecord record = new ChukwaRecord();

      String[] list = body.split(",");
      for(String pair : list) {
        String[] kv = pair.split("=");
        record.add(kv[0], kv[1]);
      }
      record.add("cluster", chunk.getTag("cluster"));
      buildGenericRecord(record, d.getTime(), "summary");
      output.collect(key, record);
    } catch (ParseException e) {
      log.warn("Wrong format in JobSummary [" + recordEntry + "]",
          e);
      throw e;
    } catch (IOException e) {
      log.warn("Unable to collect output in JobSummary ["
          + recordEntry + "]", e);
      throw e;
    } catch (Exception e) {
      log.warn("Wrong format in JobSummary [" + recordEntry + "]",
          e);
      throw e;
    }

  }

  protected void buildGenericRecord(ChukwaRecord record,
      long timestamp, String dataSource) {
    calendar.setTimeInMillis(timestamp);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    key.setKey("" + calendar.getTimeInMillis() + "/" + record.getValue("jobId") + "/"
        + timestamp);
    key.setReduceType(dataSource);
    record.setTime(timestamp);

    record.add(Record.tagsField, chunk.getTags());
    record.add(Record.sourceField, chunk.getSource());
    record.add(Record.applicationField, chunk.getStreamName());

  }
  
  public String getDataType() {
    return JobSummary.class.getName();
  }

}
