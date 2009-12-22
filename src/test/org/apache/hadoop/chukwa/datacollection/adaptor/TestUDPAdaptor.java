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

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.chukwa.*;
import java.net.*;


public class TestUDPAdaptor extends TestCase implements ChunkReceiver {
  volatile boolean receivedOK = false;
  String STR = "a short string";
  
  public void testUDP() throws Exception {
    UDPAdaptor u = new UDPAdaptor();
    u.parseArgs("Test", "0", AdaptorManager.NULL);
    u.start("id", "Test", 0, this);
    
    DatagramSocket send = new DatagramSocket();
    byte[] buf = STR.getBytes();
    DatagramPacket p = new DatagramPacket(buf, buf.length);
    p.setSocketAddress(new InetSocketAddress("127.0.0.1",u.portno));
    send.send(p);
    
    synchronized(this) {
      wait(1000);
    }
    assertTrue(receivedOK);
  }
  
  public void add(Chunk c) {
    assertTrue(c.getDataType().equals("Test"));
    assertEquals(c.getSeqID(), c.getData().length);
    assertTrue(STR.equals(new String(c.getData())));
    receivedOK= true;
    synchronized(this) {
      notify();
    }
   }

}
