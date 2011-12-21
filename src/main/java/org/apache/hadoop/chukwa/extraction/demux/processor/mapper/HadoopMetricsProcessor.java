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
@Table(name="Hadoop",columnFamily="jvm_metrics"),
@Table(name="Hadoop",columnFamily="mapred_metrics"),
@Table(name="Hadoop",columnFamily="dfs_metrics"),
@Table(name="Hadoop",columnFamily="dfs_namenode"),
@Table(name="Hadoop",columnFamily="dfs_FSNamesystem"),
@Table(name="Hadoop",columnFamily="dfs_datanode"),
@Table(name="Hadoop",columnFamily="mapred_jobtracker"),
@Table(name="Hadoop",columnFamily="mapred_shuffleInput"),
@Table(name="Hadoop",columnFamily="mapred_shuffleOutput"),
@Table(name="Hadoop",columnFamily="mapred_tasktracker"),
@Table(name="Hadoop",columnFamily="rpc_metrics")
})
public class HadoopMetricsProcessor extends AbstractProcessor {
//  public static final String jvm = "jvm_metrics";
//  public static final String mapred = "mapred_metrics";
//  public static final String dfs = "dfs_metrics";
//  public static final String namenode = "dfs_namenode";
//  public static final String fsdir = "dfs_FSDirectory";
//  public static final String fsname = "dfs_FSNamesystem";
//  public static final String datanode = "dfs_datanode";
//  public static final String jobtracker = "mapred_jobtracker";
//  public static final String shuffleIn = "mapred_shuffleInput";
//  public static final String shuffleOut = "mapred_shuffleOutput";
//  public static final String tasktracker = "mapred_tasktracker";
//  public static final String mr = "mapred_job";
  
  static Logger log = Logger.getLogger(HadoopMetricsProcessor.class);
  static final String chukwaTimestampField = "chukwa_timestamp";
  static final String contextNameField = "contextName";
  static final String recordNameField = "recordName";

  private SimpleDateFormat sdf = null;

  public HadoopMetricsProcessor() {
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
      body = body.replaceAll("\n", "");
      // log.info("record [" + recordEntry + "] body [" + body +"]");
      Date d = sdf.parse(dStr);

      start = body.indexOf('{');
      JSONObject json = (JSONObject) JSONValue.parse(body.substring(start));

      ChukwaRecord record = new ChukwaRecord();
      StringBuilder datasource = new StringBuilder();
      String contextName = null;
      String recordName = null;

      Iterator<String> ki = json.keySet().iterator();
      while (ki.hasNext()) {
        String keyName = ki.next();
        if (chukwaTimestampField.intern() == keyName.intern()) {
          d = new Date((Long) json.get(keyName));
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(d.getTime());
          cal.set(Calendar.SECOND, 0);
          cal.set(Calendar.MILLISECOND, 0);
          d.setTime(cal.getTimeInMillis());
        } else if (contextNameField.intern() == keyName.intern()) {
          contextName = (String) json.get(keyName);
        } else if (recordNameField.intern() == keyName.intern()) {
          recordName = (String) json.get(keyName);
          record.add(keyName, json.get(keyName).toString());
        } else {
          if(json.get(keyName)!=null) {
            record.add(keyName, json.get(keyName).toString());
          }
        }
      }
      if(contextName!=null) {
        datasource.append(contextName);
        datasource.append("_");
      }
      datasource.append(recordName);
      record.add("cluster", chunk.getTag("cluster"));
      if(contextName!=null && contextName.equals("jvm")) {
        buildJVMRecord(record, d.getTime(), datasource.toString());        
      } else {
        buildGenericRecord(record, null, d.getTime(), datasource.toString());
      }
      output.collect(key, record);
    } catch (ParseException e) {
      log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry + "]",
          e);
      throw e;
    } catch (IOException e) {
      log.warn("Unable to collect output in HadoopMetricsProcessor ["
          + recordEntry + "]", e);
      throw e;
    } catch (Exception e) {
      log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry + "]",
          e);
      throw e;
    }

  }

  protected void buildJVMRecord(ChukwaRecord record, long timestamp, String dataSource) {
    calendar.setTimeInMillis(timestamp);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    key.setKey("" + calendar.getTimeInMillis() + "/" + chunk.getSource() + ":" + 
        record.getValue("processName")+ "/" + timestamp);
    key.setReduceType(dataSource);
    record.setTime(timestamp);

    record.add(Record.tagsField, chunk.getTags());
    record.add(Record.sourceField, chunk.getSource());
    record.add(Record.applicationField, chunk.getStreamName());
  }
  
  public String getDataType() {
    return HadoopMetricsProcessor.class.getName();
  }

}
