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
<%@ page import = " org.apache.hadoop.chukwa.util.XssFilter" %>
<%
   XssFilter xf = new XssFilter(request);
   response.setHeader("boxId", xf.getParameter("boxId"));
   if(xf.getParameter("type").equals("list")) {
%>
<IFRAME id="<%= xf.getParameter("boxId") %>iframe" src="/hicc/jsp/job_viewer.jsp?<%= xf.filter(request.getQueryString()) %>" width="100%" frameborder="0" height="400" scrolling="no"></IFRAME>
<% } else { %>
<IFRAME id="<%= xf.getParameter("boxId") %>iframe" src="/hicc/jsp/event.jsp" width="100%" frameborder="0" height="600"></IFRAME>
<% } %>
