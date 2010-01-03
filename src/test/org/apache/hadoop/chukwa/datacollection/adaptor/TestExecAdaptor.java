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
import org.apache.hadoop.conf.*;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.*;
import org.apache.hadoop.chukwa.datacollection.test.*;

public class TestExecAdaptor extends TestCase {

  Connector chunks;


  public void testWithPs() throws ChukwaAgent.AlreadyRunningException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    ChukwaAgent agent = new ChukwaAgent(conf);
    ChunkCatcherConnector chunks = new ChunkCatcherConnector();
    chunks.start();
    String psAgentID = agent.processAddCommand(
        "add org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor ps ps aux 0");
    assertNotNull(psAgentID);
    Chunk c = chunks.waitForAChunk();
    System.out.println(new String(c.getData()));
    agent.shutdown();
  }
  
  /*
   * Buzz in a loop, starting ls every 100 ms.
   * Length of loop controlled by sleep statement near bottom of function
   */
  public void testForLeaks()  throws ChukwaAgent.AlreadyRunningException, InterruptedException {
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    ChukwaAgent agent = new ChukwaAgent(conf);

    chunks = new ConsoleOutConnector(agent, false);
    chunks.start();
    assertEquals(0, agent.adaptorCount());
    String lsID = agent.processAddCommand(
      "add exec= org.apache.hadoop.chukwa.datacollection.adaptor.ExecAdaptor Listing 100 /bin/sleep 1 0");
    Thread.sleep( 5*1000); //RAISE THIS to test longer
    System.out.println("stopped ok"); 
  }

}
