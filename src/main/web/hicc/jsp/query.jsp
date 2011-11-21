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
<%@ page import = "java.text.DecimalFormat,java.text.NumberFormat" %>
<%@ page import = "java.sql.*" %>
<%@ page import = "java.io.*" %>
<%@ page import = "org.json.*" %>
<%@ page import = "java.util.Calendar" %>
<%@ page import = "java.util.Date" %>
<%@ page import = "java.text.SimpleDateFormat" %>
<%@ page import = "java.util.*" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.ClusterConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.util.DatabaseWriter" %>
<%@ page import = "org.apache.hadoop.chukwa.database.Macro" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page import = "org.apache.hadoop.chukwa.database.DatabaseConfig" %>
<%
    XssFilter xf = new XssFilter(request);
    NumberFormat nf = new DecimalFormat("###,###,###,##0.00");
    response.setHeader("boxId", xf.getParameter("boxId"));
    response.setContentType("text/html; chartset=UTF-8//IGNORE");
    String boxId=xf.getParameter("boxId");
    String cluster = (String) session.getAttribute("cluster");
    DatabaseWriter dbw = new DatabaseWriter(cluster);
    String path = "";
    Calendar now = Calendar.getInstance();
    HashMap<String, Integer> index = new HashMap<String, Integer>();
    long start = 0;
    long end = now.getTimeInMillis();
    String startS="";
    String endS="";
    String[] columns = xf.getParameterValues("column");
    TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
    startS = time.getStartTimeText();
    endS = time.getEndTimeText();
    start = time.getStartTime();
    end = time.getEndTime();
    String queryMacro = session.getAttribute("query_"+xf.getParameter("boxId")).toString();
    Macro mp = new Macro(start,end,queryMacro, request);
    int pageNum = 0;
    int rp = 15;
    if(xf.getParameter("page")!=null) {
      pageNum = Integer.parseInt(xf.getParameter("page"));
      rp = Integer.parseInt(xf.getParameter("rp")); 
    }
    int total=0;
    if(xf.getParameter("total")!=null) {
      total = Integer.parseInt(xf.getParameter("total"));
    }
    StringBuilder query = new StringBuilder();
    String q = mp.toString();
    query.append(q);
    
    if(q.indexOf("select ")!=-1 && q.indexOf("limit")==-1) {
      query.append(" limit ");
      query.append(pageNum);
      query.append(",");
      query.append(rp);
    }
    
    ResultSet rs = dbw.query(query.toString());
    ResultSetMetaData rmeta = rs.getMetaData();
    int col = rmeta.getColumnCount();
    JSONObject data = new JSONObject();
    JSONArray rows = new JSONArray();
    while(rs.next()) {
      JSONArray cells = new JSONArray();
      for(int i=1;i<=col;i++) {
        if(rmeta.getColumnType(i)==java.sql.Types.BIGINT) {
          cells.put(nf.format(rs.getBigDecimal(i)));
        } else if(rmeta.getColumnType(i)==java.sql.Types.TINYINT ||
          rmeta.getColumnType(i)==java.sql.Types.INTEGER) {
          cells.put(nf.format(rs.getInt(i)));
        } else if(rmeta.getColumnType(i)==java.sql.Types.FLOAT ||
          rmeta.getColumnType(i)==java.sql.Types.DOUBLE ||
          rmeta.getColumnType(i)==java.sql.Types.DECIMAL ) {
          cells.put(nf.format(rs.getDouble(i)));
        } else {
          cells.put(rs.getString(i));
        }
      }
      JSONObject row = new JSONObject();
      if(rs.getString(1)!=null) {
        row.put("id",rs.getString(1));
      } else {
        row.put("id","null");
      }
      row.put("cell",cells);
      rows.put(row);
    }
    data.put("page",pageNum);
    data.put("rows",rows);
    data.put("total",total);
    dbw.close();
    out.println(data.toString());
%>
