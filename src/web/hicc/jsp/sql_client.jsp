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
<%@ page import = "java.util.regex.Pattern" %>
<%@ page import = "java.util.regex.Matcher" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page import = "org.apache.hadoop.chukwa.util.ExceptionUtil" %>
<%
  RequestDispatcher disp = null;
  XssFilter xf = new XssFilter(request);
  response.setContentType("text/html; chartset=UTF-8//IGNORE");
  response.setHeader("boxId", xf.getParameter("boxId"));
  Pattern pDelete = Pattern.compile("delete*from", Pattern.CASE_INSENSITIVE);
  Pattern pInsert = Pattern.compile("insert ", Pattern.CASE_INSENSITIVE);
  Pattern pReplace = Pattern.compile("replace ", Pattern.CASE_INSENSITIVE);
  Pattern pDrop = Pattern.compile("drop ", Pattern.CASE_INSENSITIVE);
  Pattern pUpdate = Pattern.compile("update ", Pattern.CASE_INSENSITIVE);
  try {
    String format="table";
    if(request.getParameter("render")!=null) {
      format=xf.getParameter("render");
    }
    if(pDelete.matcher(xf.getParameter("query")).matches() ||
       pInsert.matcher(xf.getParameter("query")).matches() ||
       pReplace.matcher(xf.getParameter("query")).matches() ||
       pDrop.matcher(xf.getParameter("query")).matches() ||
       pUpdate.matcher(xf.getParameter("query")).matches()) {
       throw new Exception("Read only query supported");
    }
    if(format.intern()=="table".intern()) {
      disp = getServletContext( ).getRequestDispatcher("/jsp/table.jsp");
      disp.forward(request, response);
    } else if(format.intern()=="area".intern() ||
        format.intern()=="bar".intern() ||
        format.intern()=="line".intern() ||
        format.intern()=="point".intern() ||
        format.intern()=="stack-area".intern()) {
      disp = getServletContext( ).getRequestDispatcher("/jsp/single-series-chart-javascript.jsp");
      disp.forward(request, response);
    }
  } catch(Exception ex) {
    out.println("Unsupported query.<pre>");
    out.println(ExceptionUtil.getStackTrace(ex));
    out.println("</pre>");
  } 
%>
