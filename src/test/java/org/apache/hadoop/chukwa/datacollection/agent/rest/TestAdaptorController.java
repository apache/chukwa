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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHandler;
import org.mortbay.jetty.Server;

import javax.servlet.ServletException;
import javax.servlet.Servlet;
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

  protected void setUp() throws Exception {
    agent = new ChukwaAgent();

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
    agent.processAddCommandE("add org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor SomeDataType 0");
    sb = new StringBuilder();
  }

  protected void tearDown() throws Exception {
    agent.shutdown();
    jettyServer.stop();
  }

  public void testGetPlainText() throws IOException, ServletException {
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.setQueryString("viewType=text");
    request.setMethod("GET");

    servlet.service(request, response);

    // assert response
    assertTextResponse(response, 1);

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
    request.setQueryString("viewType=xml");
    request.setMethod("GET");

    servlet.service(request, response);

    String responseContent = response.getContentAsString();

    // assert response
    assertXmlResponse(response, 1);

    //assert agent
    assertEquals("Incorrect total number of adaptors", 1, agent.adaptorCount());
  }

  public void testGetInvalidViewType() throws IOException, ServletException {
    request.setServletPath("/adaptor");
    request.setRequestURI(request.getContextPath() + request.getServletPath());
    request.setQueryString("viewType=unsupportedViewType");
    request.setMethod("GET");

    servlet.service(request, response);

    // assert response
    assertEquals("Unexpected response status", 400, response.getStatus());
    assertEquals("Unexpected response content",
            "Invalid viewType: unsupportedViewType", response.getContentAsString());
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
    request.setContentType("application/json;charset=UTF-8");
    request.setContent("{ \"DataType\" : \"SomeDataType\", \"AdaptorClass\" : \"org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor\", \"AdaptorParams\" : \"1000\", \"Offset\" : \"5555\" }".getBytes());

    //assert agent
    assertEquals("Incorrect total number of adaptors", 1, agent.adaptorCount());
    String initialAdaptorId = agent.getAdaptorList().keySet().iterator().next();

    request.setMethod("POST");

    servlet.service(request, response);

    // assert response
    String responseContent = assertXmlResponse(response, 1);
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
    request.setQueryString("viewType=text");
    request.setMethod("GET");
    request.setContentType(null);
    request.setContent(null);

    servlet.service(request, response);

    // assert response
    assertTextResponse(response, 2);
  }

  private String assertTextResponse(MockHttpServletResponse response,
                                  int adaptorCount)
                                  throws UnsupportedEncodingException {
    String responseContent = response.getContentAsString();

    assertEquals("Unexpected response status", 200, response.getStatus());
    //Content it correct when executed via an HTTP client, but it doesn't seem
    //to get set by the servlet
    //assertEquals("Unexpected content type", "text/plain;charset=UTF-8",
    //            response.getContentType());
    assertOccurs("Response text doesn't include correct adaptor_count", 1,
            responseContent, "adaptor_count: " + adaptorCount);
    assertOccurs("Response text doesn't include adaptor class",
            adaptorCount, responseContent,
            "adaptor_class: org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor");
    assertOccurs("Response text doesn't include data type",
            adaptorCount, responseContent, "data_type: SomeDataType");

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
    //assertEquals("Unexpected content type", "text/xml;charset=UTF-8",
    //            response.getContentType());
    assertOccurs("Response XML doesn't include correct adaptor_count", adaptorCount,
            responseContent, "Adaptor>");
    assertOccurs("Response XML doesn't include AdaptorClass", adaptorCount, responseContent,
            "<AdaptorClass>org.apache.hadoop.chukwa.datacollection.adaptor.ChukwaTestAdaptor</AdaptorClass>");
    assertOccurs("Response XML doesn't include dataType", adaptorCount, responseContent,
            "dataType=\"SomeDataType\"");

    return responseContent;
  }

  //  ** test utility methods **

  public void testPrintNvp() throws IOException {
    AdaptorController.appendNvp(sb, "foo", "bar");
    assertEquals("Unexpected NVP", "foo: bar\n", sb.toString());
  }

  public void testPrintNvpIndented() throws IOException {
    AdaptorController.appendNvp(sb, 4, "foo", "bar");
    assertEquals("Unexpected NVP", "    foo: bar\n", sb.toString());
  }

  public void testPrintNvpIndentedWithDash() throws IOException {
    AdaptorController.appendNvp(sb, 4, "- foo", "bar");
    assertEquals("Unexpected NVP", "  - foo: bar\n", sb.toString());
  }

  public void testPrintNvpIndentedWithStringLiteral() throws IOException {
    AdaptorController.appendNvp(sb, 4, "foo", "bar", true);
    assertEquals("Unexpected NVP",
            "    foo: |\n      bar\n", sb.toString());
  }

  public void testPrintStartTag() throws IOException {
    AdaptorController.appendStartTag(sb, "Foo");
    assertEquals("Unexpected XML", "<Foo>", sb.toString());
  }

  public void testPrintStartTagWithAttributes() throws IOException {
    AdaptorController.appendStartTag(sb, "Foo", "a", "A", "b", "B <");
    assertEquals("Unexpected XML",
            "<Foo a=\"A\" b=\"B &lt;\">", sb.toString());
  }

  public void testPrintElement() throws IOException {
    AdaptorController.appendElement(sb, "Foo", "Bar");
    assertEquals("Unexpected XML", "<Foo>Bar</Foo>", sb.toString());
  }

  public void testPrintElementWithAttributes() throws IOException {
    AdaptorController.appendElement(sb, "Foo", "Bar < -- />", "a", "A", "b", "B");
    assertEquals("Unexpected XML",
            "<Foo a=\"A\" b=\"B\">Bar &lt; -- /&gt;</Foo>", sb.toString());
  }

  public void testPrintEndTag() throws IOException {
    AdaptorController.appendEndTag(sb, "Foo");
    assertEquals("Unexpected XML", "</Foo>", sb.toString());
  }
}
