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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import junit.framework.TestCase;
import static org.apache.hadoop.chukwa.util.TempFileUtil.*;

public class TestFileAdaptor extends TestCase {

  Configuration conf = new Configuration();
  static File baseDir;
  File testFile;
  ChunkCatcherConnector chunks;
  
  public TestFileAdaptor() throws IOException {
    baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    conf.setInt("chukwaAgent.control.port", 0);
    conf.set("chukwaAgent.checkpoint.dir", baseDir.getCanonicalPath());
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    conf.setInt("chukwaAgent.adaptor.fileadaptor.timeoutperiod", 100);
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);
    testFile = makeTestFile("test", 10, baseDir);

    chunks = new ChunkCatcherConnector();
    chunks.start();
  }
  
  
  public void testOnce()  throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    
    assertEquals(0, agent.adaptorCount());

    String name =agent.processAddCommand("add test = FileAdaptor raw " +testFile.getCanonicalPath() + " 0");
    assertEquals(1, agent.adaptorCount());
    assertEquals(name, "adaptor_test");
    Chunk c = chunks.waitForAChunk(5000);
    assertNotNull(c);
    String dat = new String(c.getData());
    assertTrue(dat.startsWith("0 abcdefghijklmnopqrstuvwxyz"));
    assertTrue(dat.endsWith("9 abcdefghijklmnopqrstuvwxyz\n"));
    assertTrue(c.getDataType().equals("raw"));
    agent.shutdown();
  }
  
  public void testRepeatedly() throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    int tests = 10; //SHOULD SET HIGHER AND WATCH WITH lsof to find leaks

    ChukwaAgent agent = new ChukwaAgent(conf);
    for(int i=0; i < tests; ++i) {
      if(i % 100 == 0)
        System.out.println("buzzed " + i + " times");
      
      assertEquals(0, agent.adaptorCount());
      String name = agent.processAddCommand("add test = FileAdaptor raw " +testFile.getCanonicalPath() + " 0");
      assertEquals(1, agent.adaptorCount());
      assertEquals(name, "adaptor_test");
      Chunk c = chunks.waitForAChunk(5000);
      assertNotNull(c);
      String dat = new String(c.getData());
      assertTrue(dat.startsWith("0 abcdefghijklmnopqrstuvwxyz"));
      assertTrue(dat.endsWith("9 abcdefghijklmnopqrstuvwxyz\n"));
      assertTrue(c.getDataType().equals("raw"));
      while(agent.adaptorCount() > 0) {
        agent.stopAdaptor("adaptor_test", false);
        Thread.sleep(1000);
      }
    }
    agent.shutdown();
  }
  
}
