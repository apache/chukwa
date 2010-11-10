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
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

@Tables(annotations={
@Table(name="Hadoop",columnFamily="Hadoop_jvm_metrics"),
@Table(name="Hadoop",columnFamily="Hadoop_mapred_metrics"),
@Table(name="Hadoop",columnFamily="Hadoop_dfs_metrics"),
@Table(name="Hadoop",columnFamily="Hadoop_dfs_namenode"),
@Table(name="Hadoop",columnFamily="Hadoop_dfs_FSDirectory"),
@Table(name="Hadoop",columnFamily="Hadoop_dfs_FSNamesystem"),
@Table(name="Hadoop",columnFamily="Hadoop_dfs_datanode"),
@Table(name="Hadoop",columnFamily="Hadoop_mapred_jobtracker"),
@Table(name="Hadoop",columnFamily="Hadoop_mapred_shuffleInput"),
@Table(name="Hadoop",columnFamily="Hadoop_mapred_shuffleOutput"),
@Table(name="Hadoop",columnFamily="Hadoop_mapred_tasktracker"),
@Table(name="Hadoop",columnFamily="Hadoop_mapred_job"),
@Table(name="Hadoop",columnFamily="Hadoop_rpc_metrics")
})
public class HadoopMetricsProcessor extends AbstractProcessor {
  public static final String jvm = "Hadoop_jvm_metrics";
  public static final String mapred = "Hadoop_mapred_metrics";
  public static final String dfs = "Hadoop_dfs_metrics";
  public static final String namenode = "Hadoop_dfs_namenode";
  public static final String fsdir = "Hadoop_dfs_FSDirectory";
  public static final String fsname = "Hadoop_dfs_FSNamesystem";
  public static final String datanode = "Hadoop_dfs_datanode";
  public static final String jobtracker = "Hadoop_mapred_jobtracker";
  public static final String shuffleIn = "Hadoop_mapred_shuffleInput";
  public static final String shuffleOut = "Hadoop_mapred_shuffleOutput";
  public static final String tasktracker = "Hadoop_mapred_tasktracker";
  public static final String mr = "Hadoop_mapred_job";
  
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
      body.replaceAll("\n", "");
      // log.info("record [" + recordEntry + "] body [" + body +"]");
      Date d = sdf.parse(dStr);

      JSONObject json = new JSONObject(body);

      ChukwaRecord record = new ChukwaRecord();
      String datasource = null;
      String recordName = null;

      Iterator<String> ki = json.keys();
      while (ki.hasNext()) {
        String keyName = ki.next();
        if (chukwaTimestampField.intern() == keyName.intern()) {
          d = new Date(json.getLong(keyName));
          Calendar cal = Calendar.getInstance();
          cal.setTimeInMillis(d.getTime());
          cal.set(Calendar.SECOND, 0);
          cal.set(Calendar.MILLISECOND, 0);
          d.setTime(cal.getTimeInMillis());
        } else if (contextNameField.intern() == keyName.intern()) {
          datasource = "Hadoop_" + json.getString(keyName);
        } else if (recordNameField.intern() == keyName.intern()) {
          recordName = json.getString(keyName);
          record.add(keyName, json.getString(keyName));
        } else {
          record.add(keyName, json.getString(keyName));
        }
      }

      datasource = datasource + "_" + recordName;
      buildGenericRecord(record, null, d.getTime(), datasource);
      output.collect(key, record);
    } catch (ParseException e) {
      e.printStackTrace();
      log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry + "]",
          e);
      throw e;
    } catch (IOException e) {
      e.printStackTrace();
      log.warn("Unable to collect output in HadoopMetricsProcessor ["
          + recordEntry + "]", e);
      throw e;
    } catch (JSONException e) {
      e.printStackTrace();
      log.warn("Wrong format in HadoopMetricsProcessor [" + recordEntry + "]",
          e);
      throw e;
    }

  }

  public String getDataType() {
    return HadoopMetricsProcessor.class.getName();
  }

}
