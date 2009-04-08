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
<%@ page import = "java.text.DecimalFormat,java.text.NumberFormat,java.sql.*,java.io.*, org.json.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.DatabaseWriter, org.apache.hadoop.chukwa.database.Macro, org.apache.hadoop.chukwa.util.XssFilter, org.apache.hadoop.chukwa.database.DatabaseConfig"  %> 
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
    String query = "select count(*) from [mr_job] where finish_time between '[start]' and '[end]';";
    mp = new Macro(start,end,query, request);
    query = mp.toString();
    ResultSet rs = dbw.query(query);
    int total = 0;
    if(rs.next()) {
      total = rs.getInt(1);
    }
    StringBuilder queryBuilder = new StringBuilder();
    queryBuilder.append("select job_id,submit_time,launch_time,finish_time,status from [mr_job]  where finish_time between '[start]' and '[end]' ");
    if(xf.getParameter("user")!=null) {
      queryBuilder.append("and user='");
      queryBuilder.append(xf.getParameter("user"));
      queryBuilder.append("'");
    }
    int pageNum = 0;
    if(xf.getParameter("page")!=null) {
      pageNum = Integer.parseInt(xf.getParameter("page"));
      int rp = Integer.parseInt(xf.getParameter("rp"));
      queryBuilder.append(" limit ");
      queryBuilder.append((pageNum-1)*rp);
      queryBuilder.append(",");
      queryBuilder.append(rp);
    }
    mp = new Macro(start,end,queryBuilder.toString(), request);
    query = mp.toString();
    rs = dbw.query(query);
    ResultSetMetaData rmeta = rs.getMetaData();
    int col = rmeta.getColumnCount();
    JSONObject data = new JSONObject();
    JSONArray rows = new JSONArray();
    while(rs.next()) {
      JSONArray cells = new JSONArray();
      for(int i=1;i<=col;i++) {
        if(i==1) {
          StringBuilder sb = new StringBuilder();
          sb.append("<a href='/hicc/jsp/job_viewer.jsp?job_id=");
          sb.append(rs.getString(i));
          sb.append("&height=");
          sb.append(xf.getParameter("height"));
          sb.append("'>");
          sb.append(rs.getString(i));
          sb.append("</a>");
          cells.put(sb.toString());
        } else {
          if(rmeta.getColumnType(i)==java.sql.Types.BIGINT ||
            rmeta.getColumnType(i)==java.sql.Types.TINYINT ||
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
    }
    data.put("page",pageNum);
    data.put("rows",rows);
    data.put("total",total);
    dbw.close();
    out.println(data.toString());
%>
