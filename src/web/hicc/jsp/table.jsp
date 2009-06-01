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
<%@ page import = "java.sql.*" %>
<%@ page import = "java.text.NumberFormat" %>
<%@ page import = "java.util.Hashtable" %>
<%@ page import = "java.util.Enumeration" %>
<%@ page import = "java.util.Calendar" %>
<%@ page import = "java.util.Date" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page import = "org.apache.hadoop.chukwa.util.DatabaseWriter" %>
<%@ page import = "org.apache.hadoop.chukwa.database.Macro" %>
<%
  RequestDispatcher disp = null;
  XssFilter xf = new XssFilter(request);
  response.setContentType("text/html; chartset=UTF-8//IGNORE");
  response.setHeader("boxId", xf.getParameter("boxId"));
  String cluster = (String) session.getAttribute("cluster");
  DatabaseWriter dbw = new DatabaseWriter(cluster);

  TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
  long start = time.getStartTime();
  long end = time.getEndTime();

  Macro mp = new Macro(start,end,request.getParameter("query"));
  String query = mp.toString();
  ResultSet rs = dbw.query(query);
  ResultSetMetaData rmeta = rs.getMetaData();
  int col = rmeta.getColumnCount();
  rs.last();
  int size = rs.getRow();
  StringBuilder url = new StringBuilder();
  url.append("/jsp/table_viewer.jsp?");
  url.append("total=");
  url.append(size);
  url.append("&boxId=");
  url.append(xf.getParameter("boxId"));
  url.append("&report=/hicc/jsp/query.jsp&height=");
  url.append(xf.getParameter("height"));
  url.append("&query=");
  StringBuilder qid = new StringBuilder();
  qid.append("query_");
  qid.append(xf.getParameter("boxId"));
  session.setAttribute(qid.toString(),request.getParameter("query"));
  for(int i=1;i<=col;i++) {
    url.append("&column=");
    url.append(rmeta.getColumnName(i));
    url.append("&columnSize=200");
    url.append("&columnAlign=left");
  }
  disp = getServletContext( ).getRequestDispatcher(url.toString());
  disp.forward(request, response);
%>
