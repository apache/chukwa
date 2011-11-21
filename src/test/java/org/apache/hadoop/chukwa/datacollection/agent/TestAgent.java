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


import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;
import junit.framework.TestCase;
//Note this test takes a minimum of 
// 20 * 2 + 6* 20 = 160 seconds. 
public class TestAgent extends TestCase {

  public void testStopAndStart() {

    try {
      Configuration conf = new Configuration();
      conf.setInt("chukwaAgent.control.port", 0);
      ChukwaAgent agent = new ChukwaAgent(conf);
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();

      int portno = agent.getControllerPort();
      ChukwaAgentController cli = new ChukwaAgentController("localhost", portno);

      for (int i = 1; i < 20; ++i) {
        String adaptorId = cli.add(
            "org.apache.hadoop.chukwa.util.ConstRateAdaptor", "raw" + i, "2000"
                + i, 0);
        assertNotNull(adaptorId);
        Thread.sleep(2000);
        cli.removeAll();
      }
      agent.shutdown();
      conn.shutdown();
      Thread.sleep(2000);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

  public void testMultiStopAndStart() {

    try {
      Configuration conf = new Configuration();
      conf.setInt("chukwaAgent.control.port", 0);
      ChukwaAgent agent = new ChukwaAgent(conf);
      ConsoleOutConnector conn = new ConsoleOutConnector(agent, true);
      conn.start();
      int count = agent.adaptorCount();
      for (int trial = 0; trial < 20; ++trial) {
        ArrayList<String> runningAdaptors = new ArrayList<String>();

        for (int i = 1; i < 7; ++i) {
          String l = agent
              .processAddCommand("add  org.apache.hadoop.chukwa.util.ConstRateAdaptor  raw"
                  + i + " 2000" + i + " 0");
          assertTrue(l != null);
          runningAdaptors.add(l);
        }
        Thread.sleep(1000);
        for (String l : runningAdaptors)
          agent.stopAdaptor(l, false);
        Thread.sleep(5000);
        assertTrue(agent.adaptorCount() == count);
      }
      agent.shutdown();
      Thread.sleep(2000);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }

  public void testLogRotate() {

  }

}
