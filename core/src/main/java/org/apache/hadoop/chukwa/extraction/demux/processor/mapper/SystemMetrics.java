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
import java.util.Map;
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
    int actualSize = 0;
    for(int i = 0; i< cpuList.size(); i++) {
      JSONObject cpu = (JSONObject) cpuList.get(i);
      //Work around for sigar returning null sometimes for cpu metrics on pLinux
      if(cpu.get("combined") == null){
    	  continue;
      }
      actualSize++;
      combined = combined + Double.parseDouble(cpu.get("combined").toString());
      user = user + Double.parseDouble(cpu.get("user").toString());
      sys = sys + Double.parseDouble(cpu.get("sys").toString());
      idle = idle + Double.parseDouble(cpu.get("idle").toString());
      @SuppressWarnings("unchecked")
      Iterator<Map.Entry<String, ?>> keys = cpu.entrySet().iterator();
      while(keys.hasNext()) {
        Map.Entry<String, ?> entry = keys.next();
        String key = entry.getKey();
        Object value = entry.getValue();
        record.add(key + "." + i, value.toString());
      }
    }
    combined = combined / actualSize;
    user = user / actualSize;
    sys = sys / actualSize;
    idle = idle / actualSize;
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
    @SuppressWarnings("unchecked")
    Iterator<Map.Entry<String, ?>> memKeys = memory.entrySet().iterator();
    while(memKeys.hasNext()) {
      Map.Entry<String, ?> entry = memKeys.next();
      String key = entry.getKey();
      Object value = entry.getValue();
      record.add(key, value.toString());
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "memory");
    output.collect(key, record);    

    record = new ChukwaRecord();
    JSONObject swap = (JSONObject) json.get("swap");
    @SuppressWarnings("unchecked")
    Iterator<Map.Entry<String, ?>> swapKeys = swap.entrySet().iterator();
    while(swapKeys.hasNext()) {
      Map.Entry<String, ?> entry = swapKeys.next();
      String key = entry.getKey();
      Object value = entry.getValue();
      record.add(key, value.toString());
    }
    buildGenericRecord(record, null, cal.getTimeInMillis(), "swap");
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
      @SuppressWarnings("unchecked")
      Iterator<Map.Entry<String, ?>> keys = netIf.entrySet().iterator();
      while(keys.hasNext()) {
        Map.Entry<String, ?> entry = keys.next();
        String key = entry.getKey();
        Object value = entry.getValue();
        record.add(key + "." + i, value.toString());
        if(i!=0) {
          if(key.equals("RxBytes")) {
            rxBytes = rxBytes + (Long) value;
          } else if(key.equals("RxDropped")) {
            rxDropped = rxDropped + (Long) value;
          } else if(key.equals("RxErrors")) {          
            rxErrors = rxErrors + (Long) value;
          } else if(key.equals("RxPackets")) {
            rxPackets = rxPackets + (Long) value;
          } else if(key.equals("TxBytes")) {
            txBytes = txBytes + (Long) value;
          } else if(key.equals("TxCollisions")) {
            txCollisions = txCollisions + (Long) value;
          } else if(key.equals("TxErrors")) {
            txErrors = txErrors + (Long) value;
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
    double total = 0;
    double used = 0;
    record = new ChukwaRecord();
    JSONArray diskList = (JSONArray) json.get("disk");
    for(int i = 0;i < diskList.size(); i++) {
      JSONObject disk = (JSONObject) diskList.get(i);
      @SuppressWarnings("unchecked")
      Iterator<Map.Entry<String, ?>> keys = disk.entrySet().iterator();
      while(keys.hasNext()) {
        Map.Entry<String, ?> entry = keys.next();
        String key = entry.getKey();
        Object value = entry.getValue();
        record.add(key + "." + i, value.toString());
        if(key.equals("ReadBytes")) {
          readBytes = readBytes + (Long) value;
        } else if(key.equals("Reads")) {
          reads = reads + (Long) value;
        } else if(key.equals("WriteBytes")) {
          writeBytes = writeBytes + (Long) value;
        } else if(key.equals("Writes")) {
          writes = writes + (Long) value;
        }  else if(key.equals("Total")) {
          total = total + (Long) value;
        } else if(key.equals("Used")) {
          used = used + (Long) value;
        }
      }
    }
    double percentUsed = used/total; 
    record.add("ReadBytes", Double.toString(readBytes));
    record.add("Reads", Double.toString(reads));
    record.add("WriteBytes", Double.toString(writeBytes));
    record.add("Writes", Double.toString(writes));
    record.add("Total", Double.toString(total));
    record.add("Used", Double.toString(used));
    record.add("PercentUsed", Double.toString(percentUsed));
    buildGenericRecord(record, null, cal.getTimeInMillis(), "disk");
    output.collect(key, record);
    
    record = new ChukwaRecord();
    record.add("cluster", chunk.getTag("cluster"));
    buildGenericRecord(record, null, cal.getTimeInMillis(), "tags");
    output.collect(key, record);
  }

}
