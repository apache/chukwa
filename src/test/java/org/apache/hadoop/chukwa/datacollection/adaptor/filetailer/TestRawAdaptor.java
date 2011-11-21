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
package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;


import java.io.*;

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import java.util.Map;
import java.util.Iterator;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.adaptor.*;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.chukwa.util.TempFileUtil.*;

public class TestRawAdaptor extends TestCase {
  ChunkCatcherConnector chunks;

  public TestRawAdaptor() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  public void testRawAdaptor() throws Exception {
    System.out.println("testing raw fta");
    runTest("FileTailingAdaptor"); 
  }


  public void testLWRawAdaptor() throws Exception {
    System.out.println("testing lightweight fta");
    runTest("LWFTAdaptor"); 
  }
  

  public void testRotAdaptor() throws Exception {
    System.out.println("testing lightweight fta");
    runTest("LWFTAdaptor"); 
  }

  public void runTest(String name) throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {

    // Remove any adaptor left over from previous run
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);
    ChukwaAgent agent = new ChukwaAgent(conf);

    File testFile = makeTestFile("chukwaRawTest", 80, 
        new File(System.getProperty("test.build.data", "/tmp")));
    
    String adaptorId = agent
        .processAddCommand("add org.apache.hadoop.chukwa.datacollection.adaptor."
            +"filetailer." + name
            + " raw " + testFile + " 0");
    assertNotNull(adaptorId);
    Chunk c = chunks.waitForAChunk(1000);
    assertNotNull(c);
    assertEquals(testFile.length(), c.getData().length);
    assertTrue(c.getDataType().equals("raw"));
    assertTrue(c.getRecordOffsets().length == 1);
    assertTrue(c.getSeqID() == testFile.length());
    
    c = chunks.waitForAChunk(1000);
    assertNull(c);
    
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
  }


}
