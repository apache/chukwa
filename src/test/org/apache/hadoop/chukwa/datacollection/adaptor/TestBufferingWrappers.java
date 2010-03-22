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

import static org.apache.hadoop.chukwa.util.TempFileUtil.makeTestFile;
import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;

public class TestBufferingWrappers extends TestCase {

  Configuration conf = new Configuration();
  static File baseDir;
  ChunkCatcherConnector chunks;
  
  public TestBufferingWrappers() throws IOException {
    baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    conf.setInt("chukwaAgent.control.port", 0);
    conf.set("chukwaAgent.checkpoint.dir", baseDir.getCanonicalPath());
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    conf.setInt("chukwaAgent.adaptor.fileadaptor.timeoutperiod", 100);
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);

    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  public void testMBResendAfterStop() throws Exception{
    resendAfterStop("MemBuffered");
  }

  public void testWriteaheadResendAfterStop() throws Exception{
    resendAfterStop("WriteaheadBuffered");
  }

  
  //start a wrapped FileAdaptor. Pushes a chunk. Stop it and restart.
  //chunk hasn't been acked, so should get pushed again.
  //we delete the file and also change the data type each time through the loop
  //to make sure we get the cached chunk.
  public void resendAfterStop(String adaptor)  throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    String ADAPTORID = "adaptor_test" + System.currentTimeMillis(); 
    String STR = "test data";
    int PORTNO = 9878;
    DatagramSocket send = new DatagramSocket();
    byte[] buf = STR.getBytes();
    DatagramPacket p = new DatagramPacket(buf, buf.length);
    p.setSocketAddress(new InetSocketAddress("127.0.0.1",PORTNO));
    
    assertEquals(0, agent.adaptorCount());
    String name =agent.processAddCommand("add "+ ADAPTORID + " = "+adaptor+" UDPAdaptor raw "+PORTNO+ " 0");
    assertEquals(name, ADAPTORID);
    Thread.sleep(500);
    send.send(p);
    
    for(int i=0; i< 5; ++i) {
      Chunk c = chunks.waitForAChunk(5000);
      System.out.println("received " + i);
      assertNotNull(c);
      String dat = new String(c.getData());
      assertTrue(dat.equals(STR));
      assertTrue(c.getDataType().equals("raw"));
      assertEquals(c.getSeqID(), STR.length());
      
      agent.stopAdaptor(name, AdaptorShutdownPolicy.RESTARTING);
      Thread.sleep(500); //for socket to deregister
      name =agent.processAddCommand("add "+ADAPTORID + " = "+adaptor+" UDPAdaptor raw "+PORTNO + " 0");
      assertEquals(name, ADAPTORID);
    }
    Chunk c = chunks.waitForAChunk(5000);

    Thread.sleep(500);
    
    buf = "different data".getBytes();
    p = new DatagramPacket(buf, buf.length);   
    p.setSocketAddress(new InetSocketAddress("127.0.0.1",PORTNO));
    send.send(p);
    c = chunks.waitForAChunk(5000);
    assertNotNull(c);
    assertEquals(buf.length + STR.length(), c.getSeqID());
    
    agent.stopAdaptor(name, true);
    assertEquals(0, agent.adaptorCount());
    Thread.sleep(500);//before re-binding
    agent.shutdown();
  }
}
