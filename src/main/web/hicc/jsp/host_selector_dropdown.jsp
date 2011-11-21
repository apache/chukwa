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
<%@ page import = "javax.servlet.http.*" %>
<%@ page import = "java.sql.*" %>
<%@ page import = "java.io.*" %>
<%@ page import = "java.util.Calendar" %>
<%@ page import = "java.util.Date" %>
<%@ page import = "java.text.SimpleDateFormat" %>
<%@ page import = "java.util.*" %>
<%@ page import = "org.json.*" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.ClusterConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.database.DatabaseConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter"  %>
<%@ page import = "org.apache.hadoop.chukwa.datastore.ChukwaHBaseStore"  %>
<%@ page import = "org.apache.commons.logging.Log" %>
<%@ page import = "org.apache.commons.logging.LogFactory" %>
<%@ page import = "org.apache.hadoop.chukwa.util.ExceptionUtil" %>
<%
  Log log = LogFactory.getLog(this.getClass());
  XssFilter xf = new XssFilter(request);
  String boxId = xf.getParameter("boxId");
  response.setHeader("boxId", xf.getParameter("boxId"));
%>
<div class="panel">
<h2>Hosts</h2>
<fieldset>
<div class="row">
<select id="<%= boxId %>group_items" name="<%= boxId %>group_items" MULTIPLE size=10 class="formSelect" style="width:200px;">
<%
    JSONArray machineNames = null;
    if(session.getAttribute("cache.machine_names")!=null) {
      machineNames = new JSONArray(session.getAttribute("cache.machine_names").toString());
    }
    String cluster=xf.getParameter("cluster");
    if(cluster!=null && !cluster.equals("null")) {
      session.setAttribute("cluster",cluster);
    } else {
      cluster = (String) session.getAttribute("cluster");
      if(cluster==null || cluster.equals("null")) {
          cluster="demo";
          session.setAttribute("cluster",cluster);
      }
    }
    ClusterConfig cc = new ClusterConfig();
    TimeHandler time = new TimeHandler(request,(String)session.getAttribute("time_zone"));
    String startS = time.getStartTimeText();
    String endS = time.getEndTimeText();
    try {
      HashMap<String, String> hosts = new HashMap<String, String>();      
      try {
        String[] selected_hosts = ((String)session.getAttribute("hosts")).split(",");
        for(String name: selected_hosts) {
          hosts.put(name,name);
        }
      } catch (NullPointerException e) {
      }
      Set<String> machines = ChukwaHBaseStore.getHostnames(cluster, time.getStartTime(), time.getEndTime(), false);
      for(String machine : machines) {
        if(hosts.containsKey(machine)) {
          out.println("<option selected>"+machine+"</option>");
        } else {
          out.println("<option>"+machine+"</option>");
        }
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
%>
</select></div>
<div class="row">
<input type="button" onClick="save_host('<%= boxId %>');" name="Apply" value="Apply" class="formButton">
</div>
</fieldset>
</div>
