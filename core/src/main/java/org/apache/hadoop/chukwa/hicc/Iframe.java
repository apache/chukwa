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
package org.apache.hadoop.chukwa.hicc;


import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.chukwa.util.XssFilter;

public class Iframe extends HttpServlet {
  public static final long serialVersionUID = 100L;

  @Override
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException { 
    resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED); 
  }

  public void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    String id;
    String height = "100%";
    XssFilter xf = null;
    xf = new XssFilter(request);
    if (xf.getParameter("boxId") != null) {
      id = xf.getParameter("boxId");
    } else {
      id = "0";
    }
    response.setContentType("text/html; chartset=UTF-8//IGNORE");
    response.setHeader("boxId", xf.getParameter("boxId"));
    PrintWriter out = response.getWriter();
    StringBuffer source = new StringBuffer();
    String requestURL = request.getRequestURL().toString().replaceFirst("iframe/", "");
    if(requestURL.indexOf("/hicc/")!=-1) {
       requestURL = requestURL.substring(requestURL.indexOf("/hicc/"));
    }
    source.append(requestURL);
    source.append("?");
    Enumeration names = request.getParameterNames();
    while (names.hasMoreElements()) {
      String key = xf.filter((String) names.nextElement());
      String[] values = xf.getParameterValues(key);
      if(values!=null) {
        for (int i = 0; i < values.length; i++) {
          source.append(key + "=" + values[i] + "&");
        }
        if (key.toLowerCase().intern() == "height".intern()) {
          height = xf.getParameter(key);
        }
      }
    }
    StringBuffer output = new StringBuffer();
    output.append("<html><body><iframe id=\"iframe");
    output.append(id);
    output.append("\" name=\"iframe");
    output.append(id);
    output.append("\" src=\"");
    output.append(source);
    output.append("\" width=\"100%\" height=\"");
    output.append(height);
    output.append("\" frameborder=\"0\" style=\"overflow: hidden\"></iframe>");
    out.println(output.toString());
  }

  public void doPost(HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    doGet(request, response);
  }
}
