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
%><?xml version="1.0" encoding="UTF-8"?>
<%@ page import = "java.util.Calendar, java.util.Date, java.sql.*, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.DatabaseWriter, org.apache.hadoop.chukwa.database.Macro, org.apache.hadoop.chukwa.database.DatabaseConfig, org.apache.hadoop.chukwa.util.XssFilter" %>
<%
    response.setContentType("text/xml");
    XssFilter xf = new XssFilter(request);
    TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
    long start = time.getStartTime();
    long end = time.getEndTime();
    String cluster = (String) session.getAttribute("cluster");
    String table = "mr_job";
    if(xf.getParameter("event_type")!=null) {
      table = xf.getParameter("event_type");
    }
    String query = "select job_id,user,submit_time,launch_time,finish_time,status from ["+table+"] where finish_time between '[start]' and '[end]'";
    Macro mp = new Macro(start,end,query, request);
    query = mp.toString();

    ArrayList<HashMap<String, Object>> events = new ArrayList<HashMap<String, Object>>();

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    DatabaseWriter dbw = new DatabaseWriter(cluster);
    try {
        rs = dbw.query(query);
        ResultSetMetaData rmeta = rs.getMetaData();
        int col = rmeta.getColumnCount();
        while (rs.next()) {
          HashMap<String, Object> event = new HashMap<String, Object>();
          long event_time=0;
          for(int i=1;i<=col;i++) {
            if(rmeta.getColumnType(i)==java.sql.Types.TIMESTAMP) {
              event.put(rmeta.getColumnName(i),rs.getTimestamp(i).getTime());
            } else {
              event.put(rmeta.getColumnName(i),rs.getString(i));
            }
          }
          events.add(event);
        }
    // Now do something with the ResultSet ....
    } catch (SQLException ex) {
      // handle any errors
      //out.println("SQLException: " + ex.getMessage());
      //out.println("SQLState: " + ex.getSQLState());
      //out.println("VendorError: " + ex.getErrorCode());
    } finally {
      // it is a good idea to release
      // resources in a finally{} block
      // in reverse-order of their creation
      // if they are no-longer needed
      dbw.close();
    }
%>
<data>
<%
    SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("submit_time");
      end=(Long)event.get("finish_time");
      String event_time = format.format(start);
      String launch_time = format.format(event.get("launch_time"));
      String event_end_time = format.format(end);
      String cell = (String) event.get("_event");
      if(event.get("status").toString().intern()=="failed".intern()) {
%>
      <event start="<%= event_time %> GMT" latestStart="<%= launch_time %> GMT" end="<%= event_end_time %> GMT" title="Job ID: <%= event.get("job_id") %>" link="/hicc/jsp/job_viewer.jsp?job_id=<%= event.get("job_id") %>" isDuration="true" color="#f00">
      Job ID: <%= event.get("job_id") %>
      User: <%= event.get("user") %>
      Status: <%= event.get("status") %>
      </event>
<%
      } else {
%>
      <event start="<%= event_time %> GMT" latestStart="<%= launch_time %> GMT" end="<%= event_end_time %> GMT" title="Job ID: <%= event.get("job_id") %>" link="/hicc/jsp/job_viewer.jsp?job_id=<%= event.get("job_id") %>" isDuration="true">
      Job ID: <%= event.get("job_id") %>
      User: <%= event.get("user") %>
      Status: <%= event.get("status") %>
      </event>
<%
      }
    } %>
</data>
