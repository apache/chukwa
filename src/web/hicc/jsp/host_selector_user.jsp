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
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter"  %>
<% XssFilter xf = new XssFilter(request);
   String boxId = xf.getParameter("boxId");
   response.setHeader("boxId", xf.getParameter("boxId"));
%>
<h2>Hosts</h2>
<textarea id="<%= boxId %>group_items" name="<%= boxId %>group_items" class="formSelect" style="width:200px;"><%
  if(session.getAttribute("hosts")!=null) {
    String[] machineNames = (session.getAttribute("hosts").toString()).split(",");
    for(String node: machineNames) {
      out.println(node);
    }
  }
%></textarea><br>
<input type="button" onClick="save_host_user('<%= boxId %>');" name="Apply" value="Apply" class="formButton">
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
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter"  %>
<% XssFilter xf = new XssFilter(request);
   String boxId = xf.getParameter("boxId");
   response.setHeader("boxId", xf.getParameter("boxId"));
%>
<h2>Hosts</h2>
<textarea id="<%= boxId %>group_items" name="<%= boxId %>group_items" class="formSelect" style="width:200px;"><%
  if(session.getAttribute("hosts")!=null) {
    String[] machineNames = (session.getAttribute("hosts").toString()).split(",");
    for(String node: machineNames) {
      out.println(node);
    }
  }
%></textarea><br>
<input type="button" onClick="save_host_user('<%= boxId %>');" name="Apply" value="Apply" class="formButton">
