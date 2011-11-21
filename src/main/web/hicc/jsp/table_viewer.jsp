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
<%
   XssFilter xf = new XssFilter(request);
   response.setHeader("boxId", xf.getParameter("boxId"));
   response.setContentType("text/html; chartset=UTF-8//IGNORE");
%>
<%@ page import = "java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*, java.util.Calendar, java.util.Date, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.extraction.engine.*, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.XssFilter" %>
<% String filter=(String)session.getAttribute("filter");
   if(filter==null) {
     filter="";
   } 
   int height = Integer.parseInt(xf.getParameter("height")) - 65;
   String url = request.getQueryString();
   StringBuffer tmp = new StringBuffer();
   tmp.append(xf.getParameter("report"));
   tmp.append("?");
   tmp.append(url);
   url = tmp.toString();
   String[] columns = xf.getParameterValues("column");
   String[] columnsSize = xf.getParameterValues("columnSize");
   String[] columnsAlign = xf.getParameterValues("columnAlign");
   if(columnsSize==null) {
     columnsSize = new String[columns.length];
     for(int i=0;i<columns.length;i++) {
       columnsSize[i]=""+columns[i].length()*12;
     }
   }
   if(columnsAlign==null) {
     columnsAlign = new String[columns.length];
     for(int i=0;i<columns.length;i++) {
       columnsAlign[i]="center";
     }
   }
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<meta content="text/html; charset=UTF-8" http-equiv="Content-Type"/>
<link href="/hicc/css/flexigrid/flexigrid.css" rel="stylesheet" type="text/css"/>
<script type="text/javascript" src="/hicc/js/jquery-1.3.2.min.js"></script>
<script type="text/javascript" src="/hicc/js/flexigrid.js"></script>
</head>
<body>
<div class="flexigrid"><table id="flex1" style="display:none"></table></div>
<script type="text/javascript">
$(document).ready(function(){
  $('#flex1').flexigrid({
    url: '<%= url %>',
    dataType: 'json',
    colModel : [
<% for(int i=0;i<columns.length;i++) { %>
    { display: '<%= columns[i] %>', name: '<%= columns[i] %>', width: '<%= columnsSize[i] %>', sortable: true, align:'<%= columnsAlign[i] %>'}, 
<% } %>
    ],
    sortorder: "asc",
    usepager: true,
    useRp: false,
    rp: 15,
    striped:false,
    showTableToggleBtn: true,
    width: 'auto',
    height: <%= height %>
  });
});
</script>
</body>
</html>
