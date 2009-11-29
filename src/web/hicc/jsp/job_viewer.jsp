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
<%@ page import = "java.text.DecimalFormat,java.text.NumberFormat,java.sql.*,java.io.*, org.json.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.DatabaseWriter, org.apache.hadoop.chukwa.database.Macro, org.apache.hadoop.chukwa.util.XssFilter, org.apache.hadoop.chukwa.database.DatabaseConfig, java.util.ArrayList"  %> 
<%
    XssFilter xf = new XssFilter(request);
    NumberFormat nf = new DecimalFormat("###,###,###,##0.00");
    response.setHeader("boxId", xf.getParameter("boxId"));
    response.setContentType("text/html; chartset=UTF-8//IGNORE"); %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta content="text/html; charset=UTF-8" http-equiv="Content-Type"/>
<link href="/hicc/css/flexigrid/flexigrid.css" rel="stylesheet" type="text/css"/>
<script type="text/javascript" src="/hicc/js/jquery-1.3.2.min.js"></script>
<script type="text/javascript" src="/hicc/js/flexigrid.js"></script>
</head>
<body>
<div class="flexigrid">
<%
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
    String query = "";
    queryBuilder.append("select * from [mr_job] where finish_time between '[start]' and '[end]' ");
    if(xf.getParameter("job_id")!=null) {
      queryBuilder = new StringBuilder();
      mp = new Macro(start,end,"[mr_job]", request);
      query = mp.toString();
      queryBuilder.append("select * from ");
      queryBuilder.append(query);
      queryBuilder.append(" where finish_time between ? and ? ");
      queryBuilder.append(" and job_id=?");
      ArrayList<Object> parms = new ArrayList<Object>();
      parms.add(new Timestamp(start));
      parms.add(new Timestamp(end));
      parms.add(xf.getParameter("job_id"));
      query = queryBuilder.toString();
      ResultSet rs = dbw.query(query, parms);
      ResultSetMetaData rmeta = rs.getMetaData();
      int col = rmeta.getColumnCount();
      JSONObject data = new JSONObject();
      JSONArray rows = new JSONArray();
      int total=0;
      while(rs.next()) {
        JSONArray cells = new JSONArray();
        out.println("<table id=\"job_summary\">");
        out.println("<tr><td>Job ID</td><td>User</td><td>Queue</td><td>Status</td><td>Submit Time</td><td>Launch Time</td><td>Finish Time</td></tr>");
        out.println("<tr>");
        for(int i=1;i<=7;i++) {
          out.println("<td>");
          out.println(rs.getString(i));
          out.println("</td>");
        }
        out.println("</tr></table>");
        out.println("<table id=\"job_counters\">");
        out.println("<tr><td colspan=2>HDFS</td><td colspan=2>Map Phase</td><td colspan=2>Combine Phase</td><td colspan=2>Reduce Phase</td></tr>");
        out.println("<tr><td>Bytes Read</td><td>");
        if(rs.getString(8)!=null) {
          out.println(rs.getString(8));
        }
        out.println("</td>");
        out.println("<td>Launched Map Tasks</td><td>");
        if(rs.getString(12)!=null) {
          out.println(rs.getString(12));
        }
        out.println("</td>");
        out.println("<td>Combine Input Records</td><td>");
        if(rs.getString(18)!=null) {
          out.println(rs.getString(18));
        }
        out.println("</td>");
        out.println("<td>Launched Reduce Tasks</td><td>");
        if(rs.getString(13)!=null) {
          out.println(rs.getString(13));
        }
        out.println("</td>");
        out.println("</tr>");
        out.println("<tr><td>Bytes Written</td><td>");
        if(rs.getString(9)!=null) {
          out.println(rs.getString(9));
        }
        out.println("</td>");
        out.println("<td>Data Local Map Tasks</td><td>");
        if(rs.getString(13)!=null) {
          out.println(rs.getString(13));
        }
        out.println("</td>");
        out.println("<td>Combine Output Records</td><td>");
        if(rs.getString(19)!=null) {
          out.println(rs.getString(19));
        }
        out.println("</td>");
        out.println("<td>Data Local Reduce Tasks</td><td>");
        if(rs.getString(14)!=null) {
          out.println(rs.getString(14));
        }
        out.println("</td></tr>");
        out.println("<tr><td colspan=2>Local</td>");
        out.println("<td>Map Input Bytes</td><td>");
        if(rs.getString(14)!=null) {
          out.println(rs.getString(14));
        }
        out.println("<td>Spilled Records</td><td>");
        if(rs.getString(20)!=null) {
          out.println(rs.getString(20));
        }
        out.println("</td>");
        out.println("<td>Reduce Input Group</td><td>");
        if(rs.getString(21)!=null) {
          out.println(rs.getString(21));
        }
        out.println("</td></tr>");
        out.println("<tr><td>Bytes Read</td><td>");
        if(rs.getString(10)!=null) {
          out.println(rs.getString(10));
        }
        out.println("</td>");
        out.println("<td>Map Output Bytes</td><td>");
        if(rs.getString(15)!=null) {
          out.println(rs.getString(15));
        }
        out.println("</td>");
        out.println("<td colspan=2></td>");
        out.println("<td>Reduce Output Groups</td><td>");
        if(rs.getString(23)!=null) {
          out.println(rs.getString(23));
        }
        out.println("</td></tr>");
        out.println("<tr><td>Bytes Written</td><td>");
        if(rs.getString(11)!=null) {
          out.println(rs.getString(11));
        }
        out.println("</td><td>Map Input Records</td><td>");
        if(rs.getString(16)!=null) {
          out.println(rs.getString(16));
        }
        out.println("</td><td colspan=2></td>");
        out.println("<td>Reduce Input Records</td><td>");
        if(rs.getString(24)!=null) {
          out.println(rs.getString(24));
        }
        out.println("</td></tr>");
        out.println("<tr><td colspan=2></td>");
        out.println("<td>Map Output Records</td><td>");
        if(rs.getString(17)!=null) {
          out.println(rs.getString(17));
        }
        out.println("</td><td colspan=2></td>");
        out.println("<td>Reduce Output Records</td><td>");
        if(rs.getString(25)!=null) {
          out.println(rs.getString(25));
        }
        out.println("</td></tr>");

        out.println("</table>");
        JSONObject job = new JSONObject(rs.getString(27));
        Iterator<String> keys = job.keys();
        out.println("<table id=\"job_conf\">");
        while(keys.hasNext()) {
          String key = (String) keys.next();
          out.println("<tr><td>");
          out.println(key);
          out.println("</td><td>");
          out.println(job.get(key));
          out.println("</td></tr>");
        }
        out.println("</table>");
      }
      dbw.close();
    } else {
      out.println("Please select a Job ID.");
    }
%>
<script type="text/javascript">
$(document).ready(function(){
  $('#job_summary').flexigrid({title:'Job Summary',height:'auto'});
  $('#job_counters').flexigrid({title:'Job Counters',height:'auto'});
  $('#job_conf').flexigrid({title:'Job Configuration'});
});
</script>
</div></body></html>
