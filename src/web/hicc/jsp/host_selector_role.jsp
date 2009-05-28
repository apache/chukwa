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
<%@ page import = "org.apache.hadoop.chukwa.database.Macro" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.ClusterConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.util.DatabaseWriter" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter"  %>
<% XssFilter xf = new XssFilter(request);
   String boxId = xf.getParameter("boxId");
   response.setHeader("boxId", xf.getParameter("boxId"));
   TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
   long start = time.getStartTime();
   long end = time.getEndTime();
   String cluster = (String) session.getAttribute("cluster");
   boolean first = false;
   String[] types = {"namenode","datanode","jobtracker","tasktracker"};
   String[] queries = {"select distinct host from [dfs_namenode] where timestamp between '[start]' and '[end]'",
                       "select distinct host from [hadoop_jvm] where process_name='datanode' and timestamp between '[start]' and '[end]'",
                       "select distinct host from [hadoop_jvm] where process_name='jobtracker' and timestamp between '[start]' and '[end]'",
                       "select distinct host from [hadoop_jvm] where process_name='tasktracker' and timestamp between '[start]' and '[end]'"};
   int i = 0;
   StringBuffer hosts = new StringBuffer();
   JSONObject roles = new JSONObject();
   for(String type : types) {
     if(xf.getParameter(type)!=null) {
       Macro mp = new Macro(start, end, queries[i]);
       String query = mp.toString();
       DatabaseWriter db = new DatabaseWriter(cluster);
       try {
         ResultSet rs = db.query(query);
         while(rs.next()) {
           if(!first) {
             hosts.append(",");
           }
           hosts.append(rs.getString(1));
           first=false;
         }
         roles.put(type,"checked");
       } catch(SQLException ex) {
       	 System.out.println("SQLException "+ ex + " on query " + query);
         // Ignore if there is no data for the cluster.
       } finally {
         db.close();
       }
     } else {
       roles.put(type,"");
     }
     i++;
   }
   if(xf.getParameter("save")!=null) {
     session.setAttribute("hosts",hosts.toString());
     session.setAttribute("host.selector.role",roles.toString());
   } else {
     if(session.getAttribute("host.selector.role")!=null) {
       roles = new JSONObject(session.getAttribute("host.selector.role").toString());
     }
   }
%>
HDFS Cluster<br>
<input type="checkbox" id="<%= boxId %>namenode" value="true" <%= roles.get("namenode") %>> Name Nodes<br>
<input type="checkbox" id="<%= boxId %>datanode" value="true" <%= roles.get("datanode") %>> Data Nodes<br><br>
Map Reduce Cluster<br>
<input type="checkbox" id="<%= boxId %>jobtracker" value="true" <%= roles.get("jobtracker") %>> Job Tracker<br>
<input type="checkbox" id="<%= boxId %>tasktracker" value="true" <%= roles.get("tasktracker") %>> Task Trackers<br><br>
<input type="button" onClick="save_host_role('<%= boxId %>');" name="Apply" value="Apply" class="formButton">
