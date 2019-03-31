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
package edu.berkeley.chukwa_xtrace;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;
import junit.framework.TestCase;
import edu.berkeley.xtrace.XTraceContext;
import edu.berkeley.xtrace.reporting.*;

public class TestXtrAdaptor extends TestCase  {
  public void testXtrAdaptor() throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException{
    Configuration conf = new Configuration();
    File baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    conf.set("chukwaAgent.checkpoint.dir", baseDir.getCanonicalPath());
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    conf.set("chukwaAgent.control.port", "0");
    ChukwaAgent agent = new ChukwaAgent(conf);
    ChunkCatcherConnector chunks = new ChunkCatcherConnector();
    chunks.start();

    System.setProperty("xtrace.reporter", "edu.berkeley.xtrace.reporting.TcpReporter");
    System.setProperty("xtrace.tcpdest", "localhost:7831");

    assertEquals(0, agent.adaptorCount());
    agent.processAddCommand("add edu.berkeley.chukwa_xtrace.XtrAdaptor XTrace TcpReportSource 0");
    assertEquals(1, agent.adaptorCount());
    
    XTraceContext.startTrace("test", "testtrace", "atag");
    XTraceContext.logEvent("test", "label");
    Chunk c = chunks.waitForAChunk();
    String report = new String(c.getData());
    assertTrue(report.contains("Agent: test"));
    assertTrue(report.contains("Tag: atag"));
    System.out.println(report);
    System.out.println("-- next chunk --- ");

    c = chunks.waitForAChunk();
    report = new String(c.getData());
    assertTrue(report.contains("Agent: test"));
    assertTrue(report.contains("Label: label"));
    System.out.println(report);

    System.out.println("OK");
    agent.shutdown();
  }

}
