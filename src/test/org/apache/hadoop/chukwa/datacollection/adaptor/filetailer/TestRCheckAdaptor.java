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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.chukwa.datacollection.adaptor.TestDirTailingAdaptor;
import org.apache.log4j.Level;

public class TestRCheckAdaptor extends TestCase implements ChunkReceiver {
  
  ChunkCatcherConnector chunks;

  public TestRCheckAdaptor() {
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }

  public void testBaseCases() throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 100);
        
//    RCheckFTAdaptor.log.setLevel(Level.DEBUG);
    File baseDir = new File(System.getProperty("test.build.data", "/tmp") + "/rcheck");
    TestDirTailingAdaptor.createEmptyDir(baseDir);
    File tmpOutput = new File(baseDir, "rotateTest.1");
    PrintWriter pw = new PrintWriter(new FileOutputStream(tmpOutput));
    pw.println("First");
    pw.close();
    Thread.sleep(1000);//to make sure mod dates are distinguishing.
    tmpOutput = new File(baseDir, "rotateTest");
    pw = new PrintWriter(new FileOutputStream(tmpOutput));
    pw.println("Second");
    pw.close();
    
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    String adaptorID = agent.processAddCommand("add lr = filetailer.RCheckFTAdaptor test " + tmpOutput.getAbsolutePath() + " 0");
    assertNotNull(adaptorID);
    
    Chunk c = chunks.waitForAChunk(2000);
    assertNotNull(c);
    assertTrue(c.getData().length == 6);
    assertTrue("First\n".equals(new String(c.getData())));
    c = chunks.waitForAChunk(2000);
    assertNotNull(c);
    assertTrue(c.getData().length == 7);    
    assertTrue("Second\n".equals(new String(c.getData())));

    pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    pw.println("Third");
    pw.close();
    c = chunks.waitForAChunk(2000);
    
    assertNotNull(c);
    assertTrue(c.getData().length == 6);    
    assertTrue("Third\n".equals(new String(c.getData())));
    Thread.sleep(1500);
    
    tmpOutput.renameTo(new File(baseDir, "rotateTest.2"));
    pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    pw.println("Fourth");
    pw.close();
    c = chunks.waitForAChunk(2000);

    assertNotNull(c);
    System.out.println("got " + new String(c.getData()));
    assertTrue("Fourth\n".equals(new String(c.getData())));

    Thread.sleep(1500);
    
    tmpOutput.renameTo(new File(baseDir, "rotateTest.3"));
    Thread.sleep(400);
    pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    pw.println("Fifth");
    pw.close();
    c = chunks.waitForAChunk(2000);
    assertNotNull(c);
    System.out.println("got " + new String(c.getData()));
    assertTrue("Fifth\n".equals(new String(c.getData())));

    agent.shutdown();
    Thread.sleep(2000);
  }
  
  
  public void testContinuously() throws Exception {
    File baseDir = new File(System.getProperty("test.build.data", "/tmp") + "/rcheck");
    TestDirTailingAdaptor.createEmptyDir(baseDir);
    File tmpOutput = new File(baseDir, "continuousTest");
    PrintWriter pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
    LWFTAdaptor.tailer.SAMPLE_PERIOD_MS = 2000;

//    RCheckFTAdaptor.log.setLevel(Level.DEBUG);
    RCheckFTAdaptor rca = new RCheckFTAdaptor();
    rca.parseArgs("Test", tmpOutput.getAbsolutePath(), AdaptorManager.NULL);
    rca.start("id", "Test", 0, this);
    

    Thread.sleep(1000);
    for(int i= 0; i < 200; ++i) {
      Thread.sleep(120);
      pw.println("This is line:" + i);
      if( i % 5 == 0)
        pw.flush();
      if(i % 20 == 0) {
        System.err.println("rotating");
        pw.close();
        tmpOutput.renameTo( new File(baseDir, "continuousTest."+(i/10)));
        pw = new PrintWriter(new FileOutputStream(tmpOutput, true));
      }
    }
    Thread.sleep(1000);

    rca.shutdown(AdaptorShutdownPolicy.HARD_STOP);
    
  }

  volatile int nextExpectedLine = 0;
  
  @Override
  public void add(Chunk event) throws InterruptedException {
//    System.out.println("got a chunk; len = " + event.getData().length);
    String[] lines = new String(event.getData()).split("\n");
    System.err.println("got chunk; " + lines.length + " lines " + event.getData().length + " bytes");
    for(String line: lines) {
      String n = line.substring(line.indexOf(':')+1);
      int i = Integer.parseInt(n);
//      System.out.println("saw "+i);
      if(i != nextExpectedLine) {
        System.err.println("lines out of order: saw " + i + " expected " + nextExpectedLine);
        System.exit(0);
        fail();
      }
      nextExpectedLine = i+1;
    
    }
  }

}
