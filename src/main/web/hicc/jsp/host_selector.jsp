<%
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
%>
<%@ page import = "java.text.SimpleDateFormat" %>
<%@ page import = "java.text.NumberFormat" %>
<%@ page import = "java.util.Hashtable" %>
<%@ page import = "java.util.Enumeration" %>
<%@ page import = "java.util.Calendar" %>
<%@ page import = "java.util.Date" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%
  RequestDispatcher disp = null;
  XssFilter xf = new XssFilter(request);
  response.setContentType("text/html; chartset=UTF-8//IGNORE");
  response.setHeader("boxId", xf.getParameter("boxId"));
  String hostSelectorType="dropdown";
  if(request.getParameter("style")!=null) {
    hostSelectorType=xf.getParameter("style");
  }
  if(hostSelectorType.intern()=="role".intern()) {
    disp = getServletContext( ).getRequestDispatcher("/jsp/host_selector_role.jsp");
    disp.forward(request, response);
  } else if(hostSelectorType.intern()=="dropdown".intern()) {
    disp = getServletContext( ).getRequestDispatcher("/jsp/host_selector_dropdown.jsp");
    disp.forward(request, response);
  } else if(hostSelectorType.intern()=="user".intern()) {
    disp = getServletContext( ).getRequestDispatcher("/jsp/host_selector_user.jsp");
    disp.forward(request, response);
  } 
%>
