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

package org.apache.hadoop.chukwa.datacollection.agent;


import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent.AlreadyRunningException;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;
import org.apache.hadoop.conf.Configuration;
import junit.framework.TestCase;
import java.net.*;
import java.io.*;

public class TestCmd extends TestCase {

  public void testAddCmdWithParam() {
    ChukwaAgent agent;
    try {
      Configuration conf = new Configuration();
      conf.set("chukwaAgent.control.port", "0");
      agent = new ChukwaAgent(conf);
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      String l = agent
          .processAddCommand("ADD org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor  chukwaTestAdaptorType 0 my param1 param2 /var/log/messages 114027");
      assertTrue(l != null);
      Adaptor adaptor = agent.getAdaptor(l);
      ChukwaTestAdaptor chukwaTestAdaptor = (ChukwaTestAdaptor) adaptor;
      assertTrue("error in type",
          "chukwaTestAdaptorType".intern() == chukwaTestAdaptor.getType()
              .intern());
      assertTrue("error in param", "0 my param1 param2 /var/log/messages"
          .intern() == chukwaTestAdaptor.getParams().intern());
      assertTrue("error in startOffset", 114027l == chukwaTestAdaptor
          .getStartOffset());
      agent.stopAdaptor(l, false);
      agent.shutdown();

      Thread.sleep(2000);
    } catch (InterruptedException e) {

    } catch (AlreadyRunningException e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

  public void testAddCmdWithoutParam1() {
    ChukwaAgent agent;
    try {
      Configuration conf = new Configuration();
      conf.set("chukwaAgent.control.port", "0");
      agent = new ChukwaAgent(conf);
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      String name = agent
          .processAddCommand("ADD org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor  chukwaTestAdaptorType 114027");
      assertTrue(name != null);
      Adaptor adaptor = agent.getAdaptor(name);
      ChukwaTestAdaptor chukwaTestAdaptor = (ChukwaTestAdaptor) adaptor;
      assertTrue("error in type",
          "chukwaTestAdaptorType".intern() == chukwaTestAdaptor.getType()
              .intern());
      assertTrue("error in param", "".intern() == chukwaTestAdaptor.getParams()
          .intern());
      assertTrue("error in startOffset", 114027l == chukwaTestAdaptor
          .getStartOffset());
      agent.stopAdaptor(name, false);
      agent.shutdown();
      Thread.sleep(2000);
    } catch (InterruptedException e) {

    } catch (AlreadyRunningException e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

  public void testAddCmdWithoutParam2() {
    ChukwaAgent agent;
    try {
      Configuration conf = new Configuration();
      conf.set("chukwaAgent.control.port", "0");
      agent = new ChukwaAgent(conf);
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      String n = agent
          .processAddCommand("ADD org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor"
              + "  chukwaTestAdaptorType 0  114027");
      assertTrue(n != null);
      Adaptor adaptor = agent.getAdaptor(n);
      ChukwaTestAdaptor chukwaTestAdaptor = (ChukwaTestAdaptor) adaptor;
      assertTrue("error in type",
          "chukwaTestAdaptorType".intern() == chukwaTestAdaptor.getType()
              .intern());
      assertTrue("error in param", "0".intern() == chukwaTestAdaptor
          .getParams().intern());
      assertTrue("error in startOffset", 114027l == chukwaTestAdaptor
          .getStartOffset());
      agent.stopAdaptor(n, false);
      agent.shutdown();
      Thread.sleep(2000);
    } catch (InterruptedException e) {

    } catch (AlreadyRunningException e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
  
  public void testStopAll() throws Exception{
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    ChukwaAgent agent = new ChukwaAgent(conf);
    ChunkCatcherConnector chunks = new ChunkCatcherConnector();
    chunks.start();
    agent.processAddCommand(
        "ADD adaptor1 = org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor"
        + "  chukwaTestAdaptorType 0");

    agent.processAddCommand(
    "ADD adaptor2 = org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor"
    + "  chukwaTestAdaptorType 0");
    assertEquals(2, agent.adaptorCount());

    Socket s = new Socket("localhost", agent.getControllerPort());
    PrintWriter bw = new PrintWriter(new OutputStreamWriter(s.getOutputStream()));
    bw.println("stopAll");
    bw.flush();
    InputStreamReader in = new InputStreamReader(s.getInputStream());
    in.read();
    assertEquals(0, agent.adaptorCount());
    agent.shutdown();
  }
}
