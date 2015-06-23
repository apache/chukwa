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
package org.apache.hadoop.chukwa.extraction.hbase;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.TimeZone;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class SystemMetrics extends AbstractProcessor {

  public SystemMetrics() throws NoSuchAlgorithmException {
    super();
  }

  @Override
  protected void parse(byte[] recordEntry) throws Throwable {
    String buffer = new String(recordEntry);
    JSONObject json = (JSONObject) JSONValue.parse(buffer);
    time = ((Long) json.get("timestamp")).longValue();
    ChukwaRecord record = new ChukwaRecord();
    JSONArray cpuList = (JSONArray) json.get("cpu");
    double combined = 0.0;
    double user = 0.0;
    double sys = 0.0;
    double idle = 0.0;
    int actualSize = 0;
    for (int i = 0; i < cpuList.size(); i++) {
      JSONObject cpu = (JSONObject) cpuList.get(i);
      // Work around for sigar returning null sometimes for cpu metrics on
      // pLinux
      if (cpu.get("combined") == null) {
        continue;
      }
      actualSize++;
      combined = combined + Double.parseDouble(cpu.get("combined").toString());
      user = user + Double.parseDouble(cpu.get("user").toString());
      sys = sys + Double.parseDouble(cpu.get("sys").toString());
      idle = idle + Double.parseDouble(cpu.get("idle").toString());
      @SuppressWarnings("unchecked")
      Iterator<String> iterator = (Iterator<String>) cpu.keySet().iterator();
      while(iterator.hasNext()) {
        String key = iterator.next();
        addRecord("cpu." + key + "." + i, cpu.get(key).toString());
      }
    }
    combined = combined / actualSize;
    user = user / actualSize;
    sys = sys / actualSize;
    idle = idle / actualSize;
    addRecord("cpu.combined", Double.toString(combined));
    addRecord("cpu.user", Double.toString(user));
    addRecord("cpu.idle", Double.toString(idle));
    addRecord("cpu.sys", Double.toString(sys));

    addRecord("Uptime", json.get("uptime").toString());
    JSONArray loadavg = (JSONArray) json.get("loadavg");
    addRecord("LoadAverage.1", loadavg.get(0).toString());
    addRecord("LoadAverage.5", loadavg.get(1).toString());
    addRecord("LoadAverage.15", loadavg.get(2).toString());

    record = new ChukwaRecord();
    JSONObject memory = (JSONObject) json.get("memory");
    @SuppressWarnings("unchecked")
    Iterator<String> memKeys = memory.keySet().iterator();
    while (memKeys.hasNext()) {
      String key = memKeys.next();
      addRecord("memory." + key, memory.get(key).toString());
    }

    record = new ChukwaRecord();
    JSONObject swap = (JSONObject) json.get("swap");
    @SuppressWarnings("unchecked")
    Iterator<String> swapKeys = swap.keySet().iterator();
    while (swapKeys.hasNext()) {
      String key = swapKeys.next();
      addRecord("swap." + key, swap.get(key).toString());
    }

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
    for (int i = 0; i < netList.size(); i++) {
      JSONObject netIf = (JSONObject) netList.get(i);
      @SuppressWarnings("unchecked")
      Iterator<String> keys = netIf.keySet().iterator();
      while (keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, netIf.get(key).toString());
        if (i != 0) {
          if (key.equals("RxBytes")) {
            rxBytes = rxBytes + (Long) netIf.get(key);
          } else if (key.equals("RxDropped")) {
            rxDropped = rxDropped + (Long) netIf.get(key);
          } else if (key.equals("RxErrors")) {
            rxErrors = rxErrors + (Long) netIf.get(key);
          } else if (key.equals("RxPackets")) {
            rxPackets = rxPackets + (Long) netIf.get(key);
          } else if (key.equals("TxBytes")) {
            txBytes = txBytes + (Long) netIf.get(key);
          } else if (key.equals("TxCollisions")) {
            txCollisions = txCollisions + (Long) netIf.get(key);
          } else if (key.equals("TxErrors")) {
            txErrors = txErrors + (Long) netIf.get(key);
          } else if (key.equals("TxPackets")) {
            txPackets = txPackets + (Long) netIf.get(key);
          }
        }
      }
    }

    addRecord("network.RxBytes", Double.toString(rxBytes));
    addRecord("network.RxDropped", Double.toString(rxDropped));
    addRecord("network.RxErrors", Double.toString(rxErrors));
    addRecord("network.RxPackets", Double.toString(rxPackets));
    addRecord("network.TxBytes", Double.toString(txBytes));
    addRecord("network.TxCollisions", Double.toString(txCollisions));
    addRecord("network.TxErrors", Double.toString(txErrors));
    addRecord("network.TxPackets", Double.toString(txPackets));

    double readBytes = 0;
    double reads = 0;
    double writeBytes = 0;
    double writes = 0;
    double total = 0;
    double used = 0;
    record = new ChukwaRecord();
    JSONArray diskList = (JSONArray) json.get("disk");
    for (int i = 0; i < diskList.size(); i++) {
      JSONObject disk = (JSONObject) diskList.get(i);
      Iterator<String> keys = disk.keySet().iterator();
      while (keys.hasNext()) {
        String key = keys.next();
        record.add(key + "." + i, disk.get(key).toString());
        if (key.equals("ReadBytes")) {
          readBytes = readBytes + (Long) disk.get("ReadBytes");
        } else if (key.equals("Reads")) {
          reads = reads + (Long) disk.get("Reads");
        } else if (key.equals("WriteBytes")) {
          writeBytes = writeBytes + (Long) disk.get("WriteBytes");
        } else if (key.equals("Writes")) {
          writes = writes + (Long) disk.get("Writes");
        } else if (key.equals("Total")) {
          total = total + (Long) disk.get("Total");
        } else if (key.equals("Used")) {
          used = used + (Long) disk.get("Used");
        }
      }
    }
    double percentUsed = used / total;
    addRecord("disk.ReadBytes", Double.toString(readBytes));
    addRecord("disk.Reads", Double.toString(reads));
    addRecord("disk.WriteBytes", Double.toString(writeBytes));
    addRecord("disk.Writes", Double.toString(writes));
    addRecord("disk.Total", Double.toString(total));
    addRecord("disk.Used", Double.toString(used));
    addRecord("disk.PercentUsed", Double.toString(percentUsed));
  }

}
