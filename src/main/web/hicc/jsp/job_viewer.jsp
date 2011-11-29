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
<%@ page import = "java.text.DecimalFormat,java.text.NumberFormat,java.sql.*,java.io.*, org.json.*, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.DatabaseWriter, org.apache.hadoop.chukwa.database.Macro, org.apache.hadoop.chukwa.util.XssFilter, org.apache.hadoop.chukwa.database.DatabaseConfig, java.util.ArrayList, org.apache.hadoop.hbase.HBaseConfiguration, org.apache.hadoop.hbase.client.HTableInterface, org.apache.hadoop.hbase.client.HTablePool, org.apache.hadoop.hbase.client.Result, org.apache.hadoop.hbase.client.ResultScanner, org.apache.hadoop.hbase.client.Scan, org.apache.hadoop.conf.Configuration"  %> 
<%! final static private Configuration hconf = HBaseConfiguration.create(); %>
<%! final static private HTablePool pool = new HTablePool(hconf, 60); %>
<%
    XssFilter xf = new XssFilter(request);
    NumberFormat nf = new DecimalFormat("###,###,###,##0.00");
    SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
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

    TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
    long start = time.getStartTime();
    long end = time.getEndTime();

    HTableInterface table = pool.getTable("Jobs");
    String family = "summary";

    Scan scan = new Scan();
    scan.addColumn(family.getBytes(), "jobId".getBytes());
    scan.addColumn(family.getBytes(), "user".getBytes());
    scan.addColumn(family.getBytes(), "submitTime".getBytes());
    scan.addColumn(family.getBytes(), "launchTime".getBytes());
    scan.addColumn(family.getBytes(), "finishTime".getBytes());
    scan.addColumn(family.getBytes(), "status".getBytes());
    scan.addColumn(family.getBytes(), "cluster".getBytes());
    scan.addColumn(family.getBytes(), "queue".getBytes());
    scan.addColumn(family.getBytes(), "numMaps".getBytes());
    scan.addColumn(family.getBytes(), "numReduces".getBytes());
    scan.addColumn(family.getBytes(), "numSlotsPerMap".getBytes());
    scan.addColumn(family.getBytes(), "numSlotsPerReduce".getBytes());
    scan.addColumn(family.getBytes(), "mapSlotSeconds".getBytes());
    scan.addColumn(family.getBytes(), "reduceSlotsSeconds".getBytes());
    scan.addColumn(family.getBytes(), "status".getBytes());
    scan.setTimeRange(start, end);
    scan.setMaxVersions();

    ResultScanner results = table.getScanner(scan);
    Iterator<Result> it = results.iterator();
%>
<table id="job_summary">
<tr>
  <td>Job ID</td>
  <td>Cluster</td>
  <td>User</td>
  <td>Queue</td>
  <td>Status</td>
  <td>Submit Time</td>
  <td>Launch Time</td>
  <td>Finish Time</td>
  <td>Number of Maps</td>
  <td>Number of Reduces</td>
  <td>Number of Slots Per Map</td>
  <td>Number of Slots Per Reduce</td>
  <td>Map Slots Seconds</td>
  <td>Reduce Slots Seconds</td>
</tr>
<%
    while(it.hasNext()) {
      Result result = it.next();
      boolean print = true;
      if(xf.getParameter("job_id")!=null) {
        print = false;
      }
      String jobId = new String(result.getValue(family.getBytes(), "jobId".getBytes()));
      if(jobId.equals(xf.getParameter("job_id"))) {
        print = true;
      }
      if(cluster!=null && cluster.equals(new String(result.getValue(family.getBytes(), "cluster".getBytes())))) {
        print = true;
      }
      if(print) {
%>
<tr>
  <td><%= new String(result.getValue(family.getBytes(), "jobId".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "cluster".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "user".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "queue".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "status".getBytes())) %></td>
  <td><%= format.format(Long.parseLong(new String(result.getValue(family.getBytes(), "submitTime".getBytes())))) %></td>
  <td><%= format.format(Long.parseLong(new String(result.getValue(family.getBytes(), "launchTime".getBytes())))) %></td>
  <td><%= format.format(Long.parseLong(new String(result.getValue(family.getBytes(), "finishTime".getBytes())))) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "numMaps".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "numReduces".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "numSlotsPerMap".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "numSlotsPerReduce".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "mapSlotSeconds".getBytes())) %></td>
  <td><%= new String(result.getValue(family.getBytes(), "reduceSlotsSeconds".getBytes())) %></td>
</tr>
<%
      }
    }
    results.close();
    table.close();
%>
</table>
<script type="text/javascript">
$(document).ready(function(){
  $('#job_summary').flexigrid({title:'Job Summary',height:'340'});
});
</script>
</div></body></html>
