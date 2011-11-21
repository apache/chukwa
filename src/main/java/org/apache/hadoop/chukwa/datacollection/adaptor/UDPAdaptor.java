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
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.FileTailingAdaptor;
import org.apache.log4j.Logger;

public class UDPAdaptor extends AbstractAdaptor {

  static Logger log = Logger.getLogger(UDPAdaptor.class);
  
  int portno;
  DatagramSocket ds;
  volatile boolean running = true;
  volatile long bytesReceived = 0;
  String source;
  
  class ListenThread extends Thread {
    public void run() {
      log.info("UDP adaptor " + adaptorID + " started on port " + portno + " offset =" + bytesReceived);
      byte[] buf = new byte[65535];
      DatagramPacket dp = new DatagramPacket(buf, buf.length);
      try {
        while(running) {
          ds.receive(dp);
          send(buf, dp);
        }
      } catch(Exception e) {
        if(running)
          log.error("can't read UDP messages in " + adaptorID, e);
      }
    }
  }
  ListenThread lt;

  public void send(byte[] buf, DatagramPacket dp) throws InterruptedException, IOException {
    byte[] trimmedBuf =  Arrays.copyOf(buf, dp.getLength());
    bytesReceived += trimmedBuf.length;
    Chunk c = new ChunkImpl(type, source, bytesReceived, trimmedBuf, UDPAdaptor.this);
    dest.add(c);
  }
  
  @Override
  public String parseArgs(String s) {
    portno = Integer.parseInt(s);
    source = "udp:"+portno;
    return s;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    try {
      bytesReceived = offset;
      ds = new DatagramSocket(portno);
      portno = ds.getLocalPort();
      lt = new ListenThread();
      lt.start();
    } catch(Exception e) {
      throw new AdaptorException(e);
    }
  }

  @Override
  public String getCurrentStatus() {
    return type + " " + portno;
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    try {
      running = false;
      ds.close();
//      if(shutdownPolicy == AdaptorShutdownPolicy.GRACEFULLY)
        lt.join();
    } catch(InterruptedException e) {}
    return bytesReceived;
  }

}
