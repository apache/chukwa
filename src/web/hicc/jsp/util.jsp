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
    Macro mp = new Macro(start,end,"[util]", request);
    int averageBy=600;
    String tmpTable = mp.toString();
    DatabaseConfig dbc = new DatabaseConfig();
    String[] tableList = dbc.findTableNameForCharts("util", start, end);
    if(tableList[0].endsWith("_week")) {
      averageBy=600;
    } else if(tableList[0].endsWith("_month")) {
      averageBy=600;
    } else if(tableList[0].endsWith("_quarter")) {
      averageBy=1800;
    } else if(tableList[0].endsWith("_year")) {
      averageBy=10800;
    } else if(tableList[0].endsWith("_decade")) {
      averageBy=43200;
    }
    StringBuilder queryBuilder = new StringBuilder();
    int pageNum = 0;
    int rp = 0;
    if(xf.getParameter("page")!=null) {
      pageNum = Integer.parseInt(xf.getParameter("page"));
      rp = Integer.parseInt(xf.getParameter("rp")); 
    }
    int total=0;
    String query = "";
    if(xf.getParameter("queue")==null) {
      query = "select count(*) from [util] where timestamp between '[start]' and '[end]' group by queue;";
      mp = new Macro(start,end,query, request);
      query = mp.toString();
      ResultSet rs = dbw.query(query);
      if(rs.next()) {
        total = rs.getInt(1);
      }
      queryBuilder.append("select queue,sum(bytes)/((unix_timestamp('[end]')-unix_timestamp('[start]'))/");
      queryBuilder.append(averageBy);
      queryBuilder.append("*2) as hdfs_usage,sum(slot_hours)/((unix_timestamp('[end]')-unix_timestamp('[start]'))/");
      queryBuilder.append(averageBy);
      queryBuilder.append("*2) as slot_hour from [util] where timestamp between '[start]' and '[end]' group by queue");
      mp = new Macro(start,end,queryBuilder.toString(), request);
      query = mp.toString();
      if(tableList.length>1) { 
        StringBuilder queryAdd = new StringBuilder();
        queryAdd.append("(select * from (");
        queryAdd.append(query);
        queryAdd.append(") as t group by queue)");
        query = queryAdd.toString();
      }
    } else {
      query = "select count(*) from [util] where timestamp between '[start]' and '[end]' and queue=? group by user;";
      mp = new Macro(start,end,query, request);
      query = mp.toString();
      ArrayList<Object> parms = new ArrayList<Object>();
      parms.add(xf.getParameter("queue"));
      ResultSet rs = dbw.query(query, parms);
      if(rs.next()) {
        total = rs.getInt(1);
      }
      queryBuilder.append("select user,sum(bytes)/((unix_timestamp('[end]')-unix_timestamp('[start]'))/");
      queryBuilder.append(averageBy);
      queryBuilder.append("*2) as hdfs_usage,sum(slot_hours)/((unix_timestamp('[end]')-unix_timestamp('[start]'))/");
      queryBuilder.append(averageBy);
      queryBuilder.append("*2) as slot_hour from [util] where timestamp between '[start]' and '[end]' and queue='");
      queryBuilder.append(xf.getParameter("queue"));
      queryBuilder.append("' group by user");
      mp = new Macro(start,end,queryBuilder.toString(), request);
      query = mp.toString();
      if(tableList.length>1) {
        StringBuilder queryAdd = new StringBuilder();
        queryAdd.append("(select * from (");
        queryAdd.append(query);
        queryAdd.append(") as t group by user)");
        query = queryAdd.toString();
      }
    }
    ResultSet rs = dbw.query(query);
    ResultSetMetaData rmeta = rs.getMetaData();
    int col = rmeta.getColumnCount();
    JSONObject data = new JSONObject();
    JSONArray rows = new JSONArray();
    while(rs.next()) {
      JSONArray cells = new JSONArray();
      for(int i=1;i<=col;i++) {
        if(xf.getParameter("queue")==null && i==1) {
          StringBuilder sb = new StringBuilder();
          sb.append("<a href='/hicc/jsp/table_viewer.jsp?report=%2Fhicc%2Fjsp%2Futil.jsp&column=User&column=HDFS Storage Usage (GB Per Hour)&column=Slot Hours&columnSize=400&columnSize=100&columnSize=100&height=");
          sb.append(xf.getParameter("height"));
          sb.append("&queue=");
          sb.append(rs.getString(i));
          sb.append("'>");
          sb.append(rs.getString(i));
          sb.append("</a>");
          cells.put(sb.toString());
        } else if(xf.getParameter("queue")!=null && i==1) {
          StringBuilder sb = new StringBuilder();
          sb.append("<a href='/hicc/jsp/table_viewer.jsp?report=%2Fhicc%2Fjsp%2Fjobs_viewer.jsp&column=Job ID&column=Submit Time&column=Launch Time&column=Finish Time&column=Status&columnSize=150&columnSize=110&columnSize=110&columnSize=110&columnSize=110&height=");
          sb.append(xf.getParameter("height"));
          sb.append("&user=");
          sb.append(rs.getString(i));
          sb.append("'>");
          sb.append(rs.getString(i));
          sb.append("</a>");
          cells.put(sb.toString());
        } else {
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
      }
      JSONObject row = new JSONObject();
      if(rs.getString(1)!=null) {
        row.put("id",rs.getString(1));
      } else {
        row.put("id","null");
      }
      row.put("cell",cells);
      rows.put(row);
      total++;
    }
    data.put("page",xf.getParameter("page"));
    data.put("rows",rows);
    data.put("total",total);
    dbw.close();
    out.println(data.toString());
%>
