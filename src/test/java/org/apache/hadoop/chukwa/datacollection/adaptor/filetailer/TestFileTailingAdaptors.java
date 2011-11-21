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

public class TestFileTailingAdaptors extends TestCase {
  ChunkCatcherConnector chunks;
  Configuration conf = new Configuration();
  File baseDir, testFile;
  
  public TestFileTailingAdaptors() throws IOException {
    chunks = new ChunkCatcherConnector();
    chunks.start();
    baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    conf.set("chukwaAgent.checkpoint.dir", baseDir.getCanonicalPath());
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);
    conf.set("chukwaAgent.control.port", "0");

    testFile = makeTestFile("chukwaCrSepTest", 80,baseDir);

  }

  public void testCrSepAdaptor() throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {
    ChukwaAgent agent = new ChukwaAgent(conf);
    // Remove any adaptor left over from previous run

    // sleep for some time to make sure we don't get chunk from existing streams
    Thread.sleep(5000);
    assertEquals(0, agent.adaptorCount());
    String adaptorId = agent
        .processAddCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8"
            + " lines " + testFile + " 0");
    assertNotNull(adaptorId);
    assertEquals(1, agent.adaptorCount());

    System.out.println("getting a chunk...");
    Chunk c = chunks.waitForAChunk();
    System.out.println("got chunk");
    while (!c.getDataType().equals("lines")) {
      c = chunks.waitForAChunk();
    }
    assertTrue(c.getSeqID() == testFile.length());
    assertTrue(c.getRecordOffsets().length == 80);
    int recStart = 0;
    for (int rec = 0; rec < c.getRecordOffsets().length; ++rec) {
      String record = new String(c.getData(), recStart,
          c.getRecordOffsets()[rec] - recStart + 1);
      assertTrue(record.equals(rec + " abcdefghijklmnopqrstuvwxyz\n"));
      recStart = c.getRecordOffsets()[rec] + 1;
    }
    assertTrue(c.getDataType().equals("lines"));
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
    Thread.sleep(2000);
  }
  
  public void testRepeatedlyOnBigFile() throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    int tests = 10; //SHOULD SET HIGHER AND WATCH WITH lsof to find leaks

    ChukwaAgent agent = new ChukwaAgent(conf);
    for(int i=0; i < tests; ++i) {
      if(i % 100 == 0)
        System.out.println("buzzed " + i + " times");
      
      assertEquals(0, agent.adaptorCount());
      agent.processAddCommand("add adaptor_test = filetailer.FileTailingAdaptor raw " +testFile.getCanonicalPath() + " 0");
      assertEquals(1, agent.adaptorCount());
      Chunk c = chunks.waitForAChunk();
      String dat = new String(c.getData());
      assertTrue(dat.startsWith("0 abcdefghijklmnopqrstuvwxyz"));
      assertTrue(dat.endsWith("9 abcdefghijklmnopqrstuvwxyz\n"));
      assertTrue(c.getDataType().equals("raw"));
      if(agent.adaptorCount() > 0)
        agent.stopAdaptor("adaptor_test", false);
    }
    agent.shutdown();
  }

  
  public void testOffsetInAdaptorName() throws IOException, ChukwaAgent.AlreadyRunningException,
  InterruptedException{
    File testFile = makeTestFile("foo", 120,baseDir);
    ChukwaAgent agent = new ChukwaAgent(conf);
    assertEquals(0, agent.adaptorCount());
    agent.processAddCommand("add test = filetailer.FileTailingAdaptor raw " +testFile.getCanonicalPath() + " 0");
    assertEquals(1, agent.adaptorCount());
    Thread.sleep(2000);
    agent.processAddCommand("add test = filetailer.FileTailingAdaptor raw " +testFile.getCanonicalPath() + " 0");
    assertEquals(1, agent.adaptorCount());
    chunks.clear();
    agent.shutdown();
  }

}
