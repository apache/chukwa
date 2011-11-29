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
<%@ page import = "java.util.Calendar, java.util.Date, java.sql.*, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.DatabaseWriter, org.apache.hadoop.chukwa.database.Macro, org.apache.hadoop.chukwa.database.DatabaseConfig, org.apache.hadoop.chukwa.util.XssFilter, org.apache.hadoop.hbase.HBaseConfiguration, org.apache.hadoop.hbase.client.HTableInterface, org.apache.hadoop.hbase.client.HTablePool, org.apache.hadoop.hbase.client.Result, org.apache.hadoop.hbase.client.ResultScanner, org.apache.hadoop.hbase.client.Scan, org.apache.hadoop.conf.Configuration" %>
<%! final static private Configuration hconf = HBaseConfiguration.create(); %>
<%! final static private HTablePool pool = new HTablePool(hconf, 60); %>
<%
    response.setContentType("text/xml");
    XssFilter xf = new XssFilter(request);
    TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
    long start = time.getStartTime();
    long end = time.getEndTime();
    String cluster = (String) session.getAttribute("cluster");

    HTableInterface table = pool.getTable("Jobs");

    String family = "summary";

    Scan scan = new Scan();
    scan.addColumn(family.getBytes(), "jobId".getBytes());
    scan.addColumn(family.getBytes(), "user".getBytes());
    scan.addColumn(family.getBytes(), "submitTime".getBytes());
    scan.addColumn(family.getBytes(), "launchTime".getBytes());
    scan.addColumn(family.getBytes(), "finishTime".getBytes());
    scan.addColumn(family.getBytes(), "status".getBytes());
    scan.setTimeRange(start, end);
    scan.setMaxVersions();

    ResultScanner results = table.getScanner(scan);
    Iterator<Result> it = results.iterator();
    ArrayList<HashMap<String, Object>> events = new ArrayList<HashMap<String, Object>>();

    while(it.hasNext()) {
      Result result = it.next();
      HashMap<String, Object> event = new HashMap<String, Object>();
      event.put("jobId", new String(result.getValue(family.getBytes(), "jobId".getBytes())));
      event.put("user", new String(result.getValue(family.getBytes(), "user".getBytes())));
      event.put("submitTime", Long.parseLong(new String(result.getValue(family.getBytes(), "submitTime".getBytes()))));
      event.put("launchTime", Long.parseLong(new String(result.getValue(family.getBytes(), "launchTime".getBytes()))));
      event.put("finishTime", Long.parseLong(new String(result.getValue(family.getBytes(), "finishTime".getBytes()))));
      event.put("status", new String(result.getValue(family.getBytes(), "status".getBytes())));
      events.add(event);
    }
    results.close();
    table.close();

%>
<data>
<%
    SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("submitTime");
      end=(Long)event.get("finishTime");
      String event_time = format.format(start);
      String launch_time = format.format((Long)event.get("launchTime"));
      String event_end_time = format.format(end);
      String cell = (String) event.get("_event");
      if(!event.get("status").toString().equals("SUCCEEDED")) {
%>
      <event start="<%= event_time %> GMT" latestStart="<%= launch_time %> GMT" end="<%= event_end_time %> GMT" title="Job ID: <%= event.get("jobId") %>" link="/hicc/jsp/job_viewer.jsp?job_id=<%= event.get("jobId") %>" isDuration="true" color="#f00">
      Job ID: <%= event.get("jobId") %>
      User: <%= event.get("user") %>
      Status: <%= event.get("status") %>
      </event>
<%
      } else {
%>
      <event start="<%= event_time %> GMT" latestStart="<%= launch_time %> GMT" end="<%= event_end_time %> GMT" title="Job ID: <%= event.get("jobId") %>" link="/hicc/jsp/job_viewer.jsp?job_id=<%= event.get("jobId") %>" isDuration="true">
      Job ID: <%= event.get("jobId") %>
      User: <%= event.get("user") %>
      Status: <%= event.get("status") %>
      </event>
<%
      }
    } %>
</data>
