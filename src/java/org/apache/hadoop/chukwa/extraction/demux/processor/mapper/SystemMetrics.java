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

/**
 * Demux parser for system metrics data collected through
 * org.apache.hadoop.chukwa.datacollection.adaptor.sigar.SystemMetrics.
 */
package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.JSONArray;
import org.json.JSONObject;

@Tables(annotations={
    @Table(name="SystemMetrics",columnFamily="cpu"),
    @Table(name="SystemMetrics",columnFamily="system"),
    @Table(name="SystemMetrics",columnFamily="memory"),
    @Table(name="SystemMetrics",columnFamily="network"),
    @Table(name="SystemMetrics",columnFamily="disk")
    })
public class SystemMetrics extends AbstractProcessor {

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    JSONObject json = new JSONObject(recordEntry);
    long timestamp = Long.parseLong(json.getString("timestamp"));
    ChukwaRecord record = new ChukwaRecord();
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTimeInMillis(timestamp);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    JSONArray cpuList = json.getJSONArray("cpu");
    for(int i = 0; i< cpuList.length(); i++) {
      JSONObject cpu = cpuList.getJSONObject(i);
      Iterator<String> keys = cpu.keys();
      while(keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, cpu.getString(key));
      }
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "cpu");
    output.collect(key, record);    

    record = new ChukwaRecord();
    record.add("Uptime", json.getString("uptime"));
    JSONArray loadavg = json.getJSONArray("loadavg");
    record.add("LoadAverage.1", loadavg.getString(0));
    record.add("LoadAverage.5", loadavg.getString(1));
    record.add("LoadAverage.15", loadavg.getString(2));
    buildGenericRecord(record, null, cal.getTimeInMillis(), "system");
    output.collect(key, record);    

    record = new ChukwaRecord();
    JSONObject memory = json.getJSONObject("memory");
    Iterator<String> memKeys = memory.keys();
    while(memKeys.hasNext()) {
      String key = memKeys.next();
      record.add(key, memory.getString(key));
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "memory");
    output.collect(key, record);    
    
    record = new ChukwaRecord();
    JSONArray netList = json.getJSONArray("network");
    for(int i = 0;i < netList.length(); i++) {
      JSONObject netIf = netList.getJSONObject(i);
      Iterator<String> keys = netIf.keys();
      while(keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, netIf.getString(key));
      }
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "network");
    output.collect(key, record);    
    
    record = new ChukwaRecord();
    JSONArray diskList = json.getJSONArray("disk");
    for(int i = 0;i < netList.length(); i++) {
      JSONObject disk = netList.getJSONObject(i);
      Iterator<String> keys = disk.keys();
      while(keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, disk.getString(key));
      }
    }    
    buildGenericRecord(record, null, cal.getTimeInMillis(), "disk");
    output.collect(key, record);    
  }

}
