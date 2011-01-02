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

package org.apache.hadoop.chukwa.datacollection.adaptor.sigar;

import java.util.HashMap;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.Uptime;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * TimerTask for collect system metrics from Hyperic Sigar.
 */
public class SigarRunner extends TimerTask {

  private static Sigar sigar = new Sigar();
  private static Logger log = Logger.getLogger(SigarRunner.class);
  private ChunkReceiver receiver = null;
  private long sendOffset = 0;
  private SystemMetrics systemMetrics;
  private HashMap<String, JSONObject> previousNetworkStats = new HashMap<String, JSONObject>();
  private HashMap<String, JSONObject> previousDiskStats = new HashMap<String, JSONObject>();
  
  public SigarRunner(ChunkReceiver dest, SystemMetrics systemMetrics) {
    receiver = dest;
    this.systemMetrics = systemMetrics;
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void run() {
    boolean skip = false;
    CpuInfo[] cpuinfo = null;
    CpuPerc[] cpuPerc = null;
    Mem mem = null;
    FileSystem[] fs = null;
    String[] netIf = null;
    Uptime uptime = null;
    double[] loadavg = null;
    JSONObject json = new JSONObject();
    try {
      // CPU utilization
      cpuinfo = sigar.getCpuInfoList();
      cpuPerc = sigar.getCpuPercList();
      JSONArray cpuList = new JSONArray();
      for (int i = 0; i < cpuinfo.length; i++) {
        JSONObject cpuMap = new JSONObject();
        cpuMap.putAll(cpuinfo[i].toMap());
        cpuMap.put("combined", cpuPerc[i].getCombined());
        cpuMap.put("user", cpuPerc[i].getUser());
        cpuMap.put("sys", cpuPerc[i].getSys());
        cpuMap.put("idle", cpuPerc[i].getIdle());
        cpuMap.put("wait", cpuPerc[i].getWait());
        cpuMap.put("nice", cpuPerc[i].getNice());
        cpuMap.put("irq", cpuPerc[i].getIrq());
        cpuList.add(cpuMap);
      }
      sigar.getCpuPerc();
      json.put("cpu", cpuList);
      
      // Uptime
      uptime = sigar.getUptime();
      json.put("uptime", uptime.getUptime());
      
      // Load Average
      loadavg = sigar.getLoadAverage();
      JSONArray load = new JSONArray();
      load.add(loadavg[0]);
      load.add(loadavg[1]);
      load.add(loadavg[2]);
      json.put("loadavg", load);

      // Memory Utilization
      mem = sigar.getMem();
      JSONObject memMap = new JSONObject();
      memMap.putAll(mem.toMap());
      json.put("memory", memMap);
      
      // Network Utilization
      netIf = sigar.getNetInterfaceList();
      JSONArray netInterfaces = new JSONArray();
      for (int i = 0; i < netIf.length; i++) {
        NetInterfaceStat net = new NetInterfaceStat();
        net = sigar.getNetInterfaceStat(netIf[i]);
        JSONObject netMap = new JSONObject();
        netMap.putAll(net.toMap());
        if(previousNetworkStats.containsKey(netIf[i])) {
          JSONObject deltaMap = previousNetworkStats.get(netIf[i]);
          deltaMap.put("RxBytes", Long.parseLong(netMap.get("RxBytes").toString()) - Long.parseLong(deltaMap.get("RxBytes").toString()));
          deltaMap.put("RxDropped", Long.parseLong(netMap.get("RxDropped").toString()) - Long.parseLong(deltaMap.get("RxDropped").toString()));
          deltaMap.put("RxErrors", Long.parseLong(netMap.get("RxErrors").toString()) - Long.parseLong(deltaMap.get("RxErrors").toString()));
          deltaMap.put("RxPackets", Long.parseLong(netMap.get("RxPackets").toString()) - Long.parseLong(deltaMap.get("RxPackets").toString()));
          deltaMap.put("TxBytes", Long.parseLong(netMap.get("TxBytes").toString()) - Long.parseLong(deltaMap.get("TxBytes").toString()));
          deltaMap.put("TxCollisions", Long.parseLong(netMap.get("TxCollisions").toString()) - Long.parseLong(deltaMap.get("TxCollisions").toString()));
          deltaMap.put("TxErrors", Long.parseLong(netMap.get("TxErrors").toString()) - Long.parseLong(deltaMap.get("TxErrors").toString()));
          deltaMap.put("TxPackets", Long.parseLong(netMap.get("TxPackets").toString()) - Long.parseLong(deltaMap.get("TxPackets").toString()));
          netInterfaces.add(deltaMap);
          skip = false;
        } else {
          netInterfaces.add(netMap);
          skip = true;
        }
        previousNetworkStats.put(netIf[i], netMap);
      }
      json.put("network", netInterfaces);

      // Filesystem Utilization
      fs = sigar.getFileSystemList();
      JSONArray fsList = new JSONArray();
      for (int i = 0; i < fs.length; i++) {
        FileSystemUsage usage = sigar.getFileSystemUsage(fs[i].getDirName());
        JSONObject fsMap = new JSONObject();
        fsMap.putAll(fs[i].toMap());
        fsMap.put("ReadBytes", usage.getDiskReadBytes());
        fsMap.put("Reads", usage.getDiskReads());
        fsMap.put("WriteBytes", usage.getDiskWriteBytes());
        fsMap.put("Writes", usage.getDiskWrites());
        if(previousDiskStats.containsKey(fs[i].getDevName())) {
          JSONObject deltaMap = previousDiskStats.get(fs[i].getDevName());
          deltaMap.put("ReadBytes", usage.getDiskReadBytes() - (Long) deltaMap.get("ReadBytes"));
          deltaMap.put("Reads", usage.getDiskReads() - (Long) deltaMap.get("Reads"));
          deltaMap.put("WriteBytes", usage.getDiskWriteBytes() - (Long) deltaMap.get("WriteBytes"));
          deltaMap.put("Writes", usage.getDiskWrites() - (Long) deltaMap.get("Writes"));
          deltaMap.putAll(fs[i].toMap());
          fsList.add(deltaMap);
          skip = false;
        } else {
          fsList.add(fsMap);
          skip = true;
        }
        previousDiskStats.put(fs[i].getDevName(), fsMap);          
      }
      json.put("disk", fsList);
      json.put("timestamp", System.currentTimeMillis());
      byte[] data = json.toString().getBytes();
      sendOffset += data.length;
      ChunkImpl c = new ChunkImpl("SystemMetrics", "Sigar", sendOffset, data, systemMetrics);
      if(!skip) {
        receiver.add(c);
      }
    } catch (Exception se) {
      log.error(ExceptionUtil.getStackTrace(se));
    }
  }

}
