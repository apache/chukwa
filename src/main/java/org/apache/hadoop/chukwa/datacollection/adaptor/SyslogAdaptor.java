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
package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.log4j.Logger;

/**
 * SyslogAdaptor reads UDP syslog message from a port and convert the message to Chukwa
 * Chunk for transport from Chukwa Agent to Chukwa Collector.  Usage:
 * 
 * add SyslogAdaptor [DataType] [Port] [SequenceNumber]
 * 
 * Syslog protocol facility name is mapped to Chukwa Data Type 
 * by SyslogAdaptor, hence each UDP port can support up to 24 data streams.
 * 
 * Data Type mapping can be overwritten in Chukwa Agent Configuration file, i.e.:
 * 
 * <property>
 *   <name>syslog.adaptor.port.9095.facility.LOCAL1</name>
 *   <value>HADOOP</value>
 * </property>
 * 
 * When demux takes place, data received on port 9095 with facility name LOCAL0 will
 * be processed by demux parser for data type "HADOOP".
 */
public class SyslogAdaptor extends UDPAdaptor {

  private final static Logger log = Logger.getLogger(SyslogAdaptor.class);
  public enum FacilityType { KERN, USER, MAIL, DAEMON, AUTH, SYSLOG, LPR, NEWS, UUCP, CRON, AUTHPRIV, FTP, NTP, AUDIT, ALERT, CLOCK, LOCAL0, LOCAL1, LOCAL2, LOCAL3, LOCAL4, LOCAL5, LOCAL6, LOCAL7 }
  public HashMap<Integer, String> facilityMap;
  DatagramSocket ds;
  volatile boolean running = true;
  volatile long bytesReceived = 0;
  
  public SyslogAdaptor() {
    facilityMap = new HashMap<Integer, String>(FacilityType.values().length);
  }
  
  public void send(byte[] buf, DatagramPacket dp) throws InterruptedException, IOException {
    StringBuilder source = new StringBuilder();
    source.append(dp.getAddress());
    String dataType = type;
    byte[] trimmedBuf =  Arrays.copyOf(buf, dp.getLength());
    String rawPRI = new String(trimmedBuf, 1, 4);
    int i = rawPRI.indexOf(">");
    if (i <= 3 && i > -1) {
      String priorityStr = rawPRI.substring(0,i);
      int priority = 0;
      int facility = 0;
      try {
        priority = Integer.parseInt(priorityStr);
        facility = (priority >> 3) << 3;
        facility = facility / 8;
        dataType = facilityMap.get(facility); 
      } catch (NumberFormatException nfe) {
        log.warn("Unsupported format detected by SyslogAdaptor:"+trimmedBuf);
      }
    }

    bytesReceived += trimmedBuf.length;
    Chunk c = new ChunkImpl(dataType, source.toString(), bytesReceived, trimmedBuf, SyslogAdaptor.this);
    dest.add(c);
  }
  
  @Override
  public String parseArgs(String s) {
    portno = Integer.parseInt(s);
    ChukwaConfiguration cc = new ChukwaConfiguration();
    for(FacilityType e : FacilityType.values()) {
      StringBuilder buffer = new StringBuilder();
      buffer.append("syslog.adaptor.port.");
      buffer.append(portno);
      buffer.append(".facility.");
      buffer.append(e.name());
      String dataType = cc.get(buffer.toString(), e.name());
      facilityMap.put(e.ordinal(), dataType);
    }
    return s;
  }

}
