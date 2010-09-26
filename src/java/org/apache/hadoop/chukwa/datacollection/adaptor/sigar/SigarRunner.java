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
import java.util.Map;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.hyperic.sigar.CpuInfo;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystem;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.NetInterfaceStat;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.hyperic.sigar.Uptime;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * TimerTask for collect system metrics from Hyperic Sigar.
 */
public class SigarRunner extends TimerTask {

  private static Sigar sigar = new Sigar();
  private static Logger log = Logger.getLogger(SigarRunner.class);
  private ChunkReceiver receiver = null;
  private long sendOffset = 0;
  private SystemMetrics systemMetrics;
  
  public SigarRunner(ChunkReceiver dest, SystemMetrics systemMetrics) {
    receiver = dest;
    this.systemMetrics = systemMetrics;
  }
  
  @Override
  public void run() {
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
        JSONObject cpuMap = new JSONObject(cpuinfo[i].toMap());
        cpuMap.put("combined", cpuPerc[i].getCombined());
        cpuMap.put("user", cpuPerc[i].getUser());
        cpuMap.put("sys", cpuPerc[i].getSys());
        cpuMap.put("idle", cpuPerc[i].getIdle());
        cpuMap.put("wait", cpuPerc[i].getWait());
        cpuMap.put("nice", cpuPerc[i].getNice());
        cpuMap.put("irq", cpuPerc[i].getIrq());
        cpuList.put(cpuMap);
      }
      sigar.getCpuPerc();
      json.put("cpu", cpuList);
      
      // Uptime
      uptime = sigar.getUptime();
      json.put("uptime", uptime.getUptime());
      
      // Load Average
      loadavg = sigar.getLoadAverage();
      JSONArray load = new JSONArray();
      load.put(loadavg[0]);
      load.put(loadavg[1]);
      load.put(loadavg[2]);
      json.put("loadavg", load);

      // Memory Utilization
      mem = sigar.getMem();
      JSONObject memMap = new JSONObject(mem.toMap());
      json.put("memory", memMap);
      
      // Network Utilization
      netIf = sigar.getNetInterfaceList();
      JSONArray netInterfaces = new JSONArray();
      for (int i = 0; i < netIf.length; i++) {
        NetInterfaceStat net = new NetInterfaceStat();
        net = sigar.getNetInterfaceStat(netIf[i]);
        JSONObject netMap = new JSONObject(net.toMap());
        netInterfaces.put(netMap);
      }
      json.put("network", netInterfaces);

      // Filesystem Utilization
      fs = sigar.getFileSystemList();
      JSONArray fsList = new JSONArray();
      for (int i = 0; i < fs.length; i++) {
        JSONObject fsMap = new JSONObject(fs[i].toMap());
        FileSystemUsage usage = sigar.getFileSystemUsage(fs[i].getDirName());
        fsMap.put("DiskReadBytes", usage.getDiskReadBytes());
        fsMap.put("DiskReads", usage.getDiskReads());
        fsMap.put("DiskWriteBytes", usage.getDiskWriteBytes());
        fsMap.put("DiskWrites", usage.getDiskWrites());
        fsList.put(fsMap);
      }
      json.put("disk", fsList);
      json.put("timestamp", System.currentTimeMillis());
      byte[] data = json.toString().getBytes();
      sendOffset += data.length;
      ChunkImpl c = new ChunkImpl("SystemMetrics", "Sigar", sendOffset, data, systemMetrics);
      
      receiver.add(c);
    } catch (Exception se) {
      log.error(ExceptionUtil.getStackTrace(se));
    }
  }

}
