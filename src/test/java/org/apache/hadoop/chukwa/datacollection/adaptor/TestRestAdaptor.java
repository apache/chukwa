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

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.json.simple.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class TestRestAdaptor extends TestCase implements ChunkReceiver {

  private Server jettyServer = null;
  private JSONObject metricsMap = new JSONObject();
  private static String args = "http://localhost:9090/metrics/instrumentation/data 2";
  private static long offset = 0;

  @Before
  public void setUp() throws Exception {
    metricsMap.put("FreeSpace", "10GB");
    metricsMap.put("UsedSpace", "90GB");
    metricsMap.put("maps_killed", "20");

    jettyServer = new Server(9090);
    Context root = new Context(jettyServer, "/metrics/instrumentation/data",
        Context.SESSIONS);
    root.addServlet(new ServletHolder(new RestServlet()), "/*");
    System.out.println(" Rest Server starting..");

    jettyServer.start();
    jettyServer.setStopAtShutdown(true);
  }

  @Test
  public void testMessageReceivedOk() throws Exception {
    RestAdaptor restAdaptor = new RestAdaptor();
    restAdaptor.parseArgs(args);
    restAdaptor.start("id", "TestRestAdaptor", 0, this);
    Thread.sleep(2000); // wait for processing
  }

  @Override
  public void add(Chunk event) throws InterruptedException {
    offset += event.getData().length;
    assertTrue(event.getDataType().equals("TestRestAdaptor"));
    assertEquals(event.getSeqID(), offset);
    assertTrue(metricsMap.toString().equals(new String(event.getData())));
  }

  @After
  public void tearDown() throws Exception {
    if (jettyServer != null) {
      jettyServer.stop();
    }
  }

  private class RestServlet extends HttpServlet {
    private static final long serialVersionUID = -8007387020169769539L;

    protected void doGet(HttpServletRequest request,
        HttpServletResponse response) throws ServletException, IOException {
      response.getWriter().write(metricsMap.toString());
    }
  }
}
