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
import java.util.HashMap;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

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
    JSONObject json = (JSONObject) JSONValue.parse(recordEntry);
    long timestamp = ((Long)json.get("timestamp")).longValue();
    ChukwaRecord record = new ChukwaRecord();
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTimeInMillis(timestamp);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    JSONArray cpuList = (JSONArray) json.get("cpu");
    double combined = 0.0;
    double user = 0.0;
    double sys = 0.0;
    double idle = 0.0;
    for(int i = 0; i< cpuList.size(); i++) {
      JSONObject cpu = (JSONObject) cpuList.get(i);
      Iterator<String> keys = cpu.keySet().iterator();
      combined = combined + Double.parseDouble(cpu.get("combined").toString());
      user = user + Double.parseDouble(cpu.get("user").toString());
      sys = sys + Double.parseDouble(cpu.get("sys").toString());
      idle = idle + Double.parseDouble(cpu.get("idle").toString());
      while(keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, cpu.get(key).toString());
      }
    }
    combined = combined / cpuList.size();
    user = user / cpuList.size();
    sys = sys / cpuList.size();
    idle = idle / cpuList.size();
    record.add("combined", Double.toString(combined));
    record.add("user", Double.toString(user));
    record.add("idle", Double.toString(idle));    
    record.add("sys", Double.toString(sys));
    buildGenericRecord(record, null, cal.getTimeInMillis(), "cpu");
    output.collect(key, record);    

    record = new ChukwaRecord();
    record.add("Uptime", json.get("uptime").toString());
    JSONArray loadavg = (JSONArray) json.get("loadavg");
    record.add("LoadAverage.1", loadavg.get(0).toString());
    record.add("LoadAverage.5", loadavg.get(1).toString());
    record.add("LoadAverage.15", loadavg.get(2).toString());
    buildGenericRecord(record, null, cal.getTimeInMillis(), "system");
    output.collect(key, record);    

    record = new ChukwaRecord();
    JSONObject memory = (JSONObject) json.get("memory");
    Iterator<String> memKeys = memory.keySet().iterator();
    while(memKeys.hasNext()) {
      String key = memKeys.next();
      record.add(key, memory.get(key).toString());
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "memory");
    output.collect(key, record);    
    
    double rxBytes = 0;
    double rxDropped = 0;
    double rxErrors = 0;
    double rxPackets = 0;
    double txBytes = 0;
    double txCollisions = 0;
    double txErrors = 0;
    double txPackets = 0;
    record = new ChukwaRecord();
    JSONArray netList = (JSONArray) json.get("network");
    for(int i = 0;i < netList.size(); i++) {
      JSONObject netIf = (JSONObject) netList.get(i);
      Iterator<String> keys = netIf.keySet().iterator();
      while(keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, netIf.get(key).toString());
        if(i!=0) {
          if(key.equals("RxBytes")) {
            rxBytes = rxBytes + (Long) netIf.get(key);
          } else if(key.equals("RxDropped")) {
            rxDropped = rxDropped + (Long) netIf.get(key);
          } else if(key.equals("RxErrors")) {          
            rxErrors = rxErrors + (Long) netIf.get(key);
          } else if(key.equals("RxPackets")) {
            rxPackets = rxPackets + (Long) netIf.get(key);
          } else if(key.equals("TxBytes")) {
            txBytes = txBytes + (Long) netIf.get(key);
          } else if(key.equals("TxCollisions")) {
            txCollisions = txCollisions + (Long) netIf.get(key);
          } else if(key.equals("TxErrors")) {
            txErrors = txErrors + (Long) netIf.get(key);
          } else if(key.equals("TxPackets")) {
            txPackets = txPackets + (Long) netIf.get(key);
          }
        }
      }
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "network");
    record.add("RxBytes", Double.toString(rxBytes));
    record.add("RxDropped", Double.toString(rxDropped));
    record.add("RxErrors", Double.toString(rxErrors));
    record.add("RxPackets", Double.toString(rxPackets));
    record.add("TxBytes", Double.toString(txBytes));
    record.add("TxCollisions", Double.toString(txCollisions));
    record.add("TxErrors", Double.toString(txErrors));
    record.add("TxPackets", Double.toString(txPackets));
    output.collect(key, record);    
    
    double readBytes = 0;
    double reads = 0;
    double writeBytes = 0;
    double writes = 0;
    record = new ChukwaRecord();
    JSONArray diskList = (JSONArray) json.get("disk");
    for(int i = 0;i < diskList.size(); i++) {
      JSONObject disk = (JSONObject) diskList.get(i);
      Iterator<String> keys = disk.keySet().iterator();
      while(keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, disk.get(key).toString());
        if(key.equals("ReadBytes")) {
          readBytes = readBytes + (Long) disk.get("ReadBytes");
        } else if(key.equals("Reads")) {
          reads = reads + (Long) disk.get("Reads");
        } else if(key.equals("WriteBytes")) {
          writeBytes = writeBytes + (Long) disk.get("WriteBytes");
        } else if(key.equals("Writes")) {
          writes = writes + (Long) disk.get("Writes");
        }
      }
    }
    record.add("ReadBytes", Double.toString(readBytes));
    record.add("Reads", Double.toString(reads));
    record.add("WriteBytes", Double.toString(writeBytes));
    record.add("Writes", Double.toString(writes));    
    buildGenericRecord(record, null, cal.getTimeInMillis(), "disk");
    output.collect(key, record);
    
    record = new ChukwaRecord();
    record.add("cluster", chunk.getTag("cluster"));
    buildGenericRecord(record, null, cal.getTimeInMillis(), "tags");
    output.collect(key, record);
  }

}
