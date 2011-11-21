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
package org.apache.hadoop.chukwa.datacollection.controller;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import java.io.IOException;
import java.util.Map;
import junit.framework.TestCase;

public class TestAgentClient extends TestCase {
  Configuration config;
  ChukwaAgent agent;
  ChukwaAgentController c;
  Connector connector;

  // consoleConnector = new ConsoleOutConnector(agent);

  protected void setUp() throws ChukwaAgent.AlreadyRunningException {
    config = new Configuration();
    agent = new ChukwaAgent(config);
    c = new ChukwaAgentController();
    connector = new ChunkCatcherConnector();
    connector.start();
  }

  protected void tearDown() {
    System.out.println("in tearDown()");
    connector.shutdown();
  }

  public void testAddFile() {
    String appType = "junit_addFileTest";
    String params = "testFile";
    try {
      // add the fileTailer to the agent using the client
      System.out.println("Adding adaptor with filename: " + params);
      String adaptorID = c.addFile(appType, params);
      System.out.println("Successfully added adaptor, id is:" + adaptorID);

      // do a list on the agent to see if the adaptor has been added for this
      // file
      Map<String, ChukwaAgentController.Adaptor> listResult = c.list();
      assertTrue(listResult.containsKey(adaptorID));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
