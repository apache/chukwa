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
package org.apache.hadoop.chukwa.datacollection.agent.rest;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.Server;

import javax.servlet.ServletException;
import javax.servlet.Servlet;
import javax.ws.rs.core.MediaType;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Tests the basic functionality of the AdaptorController.
 */
public class TestAdaptorController extends TestCase {
  protected Log log = LogFactory.getLog(getClass());

  Server jettyServer;
  ChukwaAgent agent;
  Servlet servlet;
  MockHttpServletRequest request;
  MockHttpServletResponse response;
  StringBuilder sb;
  String adaptor;

  protected void setUp() throws Exception {
    String path = System.getenv("CHUKWA_LOG_DIR");
    String[] checkpointNames = new File(path).list(new FilenameFilter() {
      public boolean accept(File dir, String name) {
        String checkPointBaseName = "chukwa_agent_checkpoint";
        return name.startsWith(checkPointBaseName);
      }
    });
    for(String cpn : checkpointNames) {
      File checkpoint = new File(path+"/"+cpn);
      if(!checkpoint.delete()) {
        Assert.fail("Fail to clean up existing check point file: "+ cpn);
      }
    }
    agent = ChukwaAgent.getAgent();
    agent.start();

    ServletHolder servletHolder = new ServletHolder(ServletContainer.class);
    servletHolder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
            "com.sun.jersey.api.core.PackagesResourceConfig");
    servletHolder.setInitParameter("com.sun.jersey.config.property.packages",
            "org.apache.hadoop.chukwa.datacollection.agent.rest");
    servletHolder.setServletHandler(new ServletHandler());

    jettyServer = new Server();

    Context root = new Context(jettyServer, "/foo/bar", Context.SESSIONS);
    root.setAttribute("ChukwaAgent", agent);
    root.addServlet(servletHolder, "/*");

    jettyServer.start();
    jettyServer.setStopAtShutdown(true);

    servlet = servletHolder.getServlet();
    request = new MockHttpServletRequest();
    request.setContextPath("/foo/bar");

    response = new MockHttpServletResponse();
    adaptor = agent.processAddCommandE("add org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor SomeDataType 0");
    sb = new StringBuilder();
  }

  protected void tearDown() throws Exception {
    agent.getAdaptor(adaptor);
    agent.shutdown();
    jettyServer.stop();
  }

  public void testGetJSON() throws IOException, ServletException {
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.addHeader("Accept", "application/json");
    request.setMethod("GET");

    servlet.service(request, response);

    //assert agent
    assertEquals("Incorrect total number of adaptors", 1, agent.adaptorCount());
  }

  private void assertOccurs(String message, int occurances, String text, String match) {

    int index = -1;
    for(int i = 0; i < occurances; i++) {
      index = text.indexOf(match, index + 1);
      assertTrue(message + ": " + text, index != -1);
    }
  }

  public void testGetXml() throws IOException, ServletException {
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.addHeader("Accept", "application/xml");
    request.setMethod("GET");

    servlet.service(request, response);

    // assert response
    assertXmlResponse(response, 1);

    //assert agent
    assertEquals("Incorrect total number of adaptors", 1, agent.adaptorCount());
  }

  public void testGetInvalidViewType() throws IOException, ServletException {
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.addHeader("Accept", "unsupportedViewType");
    request.setMethod("GET");

    servlet.service(request, response);

    // assert response
    assertEquals("Unexpected response status", 406, response.getStatus());
  }

  public void testDeleteAdaptor() throws IOException, ServletException {
    String adaptorId = agent.getAdaptorList().keySet().iterator().next();

    request.setServletPath("/adaptor/" + adaptorId);
    request.setRequestURI(request.getContextPath() + request.getServletPath());

    //assert agent
    assertEquals("Incorrect total number of adaptors", 1, agent.adaptorCount());
    request.setMethod("DELETE");

    servlet.service(request, response);

    // assert response
    assertEquals("Unexpected response status", 200, response.getStatus());

    //assert agent
    assertEquals("Incorrect total number of adaptors", 0, agent.adaptorCount());
  }

  public void testAddAdaptor() throws IOException, ServletException {
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.addHeader("Content-Type", MediaType.APPLICATION_JSON);
    request.addHeader("Accept", MediaType.APPLICATION_JSON);
    request.setMethod("POST");
    request.setContent("{ \"dataType\" : \"SomeDataType\", \"adaptorClass\" : \"org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor\", \"adaptorParams\" : \"1000\", \"offset\" : 5555 }".getBytes());
    //assert agent
    assertEquals("Incorrect total number of adaptors", 1, agent.adaptorCount());
    String initialAdaptorId = agent.getAdaptorList().keySet().iterator().next();

    servlet.service(request, response);

    // assert response
    String responseContent = assertJSONResponse(response, 1);
    String newAdaptorId = null;
    for (String id : agent.getAdaptorList().keySet()) {
      if (id != initialAdaptorId) {
        newAdaptorId = id;
        break;
      }
    }
    
    //assert agent
    assertEquals("Incorrect total number of adaptors", 2, agent.adaptorCount());

    assertOccurs("Response did not contain adaptorId", 1, responseContent, newAdaptorId);

    //assert agent
    assertEquals("Incorrect total number of adaptors", 2, agent.adaptorCount());
    
    // fire a doGet to assert that the servlet shows 2 adaptors
    request = new MockHttpServletRequest();
    response = new MockHttpServletResponse();
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.addHeader("Accept", MediaType.APPLICATION_XML);
    request.addHeader("Content-Type", MediaType.APPLICATION_XML);
    request.setMethod("GET");

    servlet.service(request, response);

    // assert response
    assertXmlResponse(response, 2);
  }

  private String assertJSONResponse(MockHttpServletResponse response,
                                  int adaptorCount)
                                  throws UnsupportedEncodingException {
    String responseContent = response.getContentAsString();
    assertEquals("Unexpected response status", 200, response.getStatus());

    JSONObject json = (JSONObject) JSONValue.parse(responseContent);
    String adaptorClass = (String) json.get("adaptorClass");
    String dataType = (String) json.get("dataType");
    assertEquals("Response text doesn't include adaptor class", 
        "org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor", adaptorClass);
    assertEquals("Response text doesn't include data type",
        "SomeDataType", dataType);

    return responseContent;
  }

  private String assertXmlResponse(MockHttpServletResponse response,
                                   int adaptorCount)
                                   throws UnsupportedEncodingException {
    String responseContent = response.getContentAsString();

    // assert response
    assertEquals("Unexpected response status", 200, response.getStatus());

    //Content it correct when executed via an HTTP client, but it doesn't seem
    //to get set by the servlet
    assertOccurs("Response XML doesn't include correct adaptor_count", adaptorCount,
            responseContent, "adaptorCount>");
    assertOccurs("Response XML doesn't include adaptorClass", adaptorCount, responseContent,
            "<adaptorClass>org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor</adaptorClass>");
    assertOccurs("Response XML doesn't include dataType", adaptorCount, responseContent,
            "<dataType>SomeDataType</dataType>");

    return responseContent;
  }

}
