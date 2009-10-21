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
<%@ page import = "org.json.*, java.net.*, java.io.*, java.util.*, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler"  %>
<% 
    session.setAttribute("time_zone","UTC");
    session.setAttribute("start","1209400695191");
    session.setAttribute("end","1209404295191");
    if(session.getAttribute("cluster")==null) {
        ClusterConfig cc = new ClusterConfig();
        Iterator ci = cc.getClusters();
        String cluster = (String) ci.next();
        session.setAttribute("cluster", cluster);
    }
    if(session.getAttribute("period")==null) {
        session.setAttribute("period","last1hr");
    }
    String machine="";
    if(session.getAttribute("hosts")==null) {
        session.setAttribute("hosts",machine);
    }
    if(session.getAttribute("time_type")==null) {
        session.setAttribute("time_type","last");
    }
    String view_name = "default";
    if(request.getParameter("view")!=null) {
        view_name=request.getParameter("view");
    }
    TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
        StringBuffer contents = new StringBuffer();
        try {
          //use buffering, reading one line at a time
          //FileReader always assumes default encoding is OK!
          FileInputStream url = new FileInputStream(System.getProperty("CHUKWA_DATA_DIR")+"/views/"+view_name+".view");
          DataInputStream in = new DataInputStream(url);
          String inputLine;
          while ((inputLine = in.readLine()) != null) {
                contents.append(inputLine);
                contents.append(System.getProperty("line.separator"));
          }
          in.close();
        } catch (IOException ex){
          ex.printStackTrace();
        }
        JSONObject view = new JSONObject(contents.toString());

        StringBuffer contents2 = new StringBuffer();
        try {
          //use buffering, reading one line at a time
          //FileReader always assumes default encoding is OK!
          FileInputStream url2 = new FileInputStream(System.getProperty("CHUKWA_DATA_DIR")+"/views/workspace_view_list.cache");
          DataInputStream in2 = new DataInputStream(url2);
          String inputLine2;
          while ((inputLine2 = in2.readLine()) != null) {
                contents2.append(inputLine2);
                contents2.append(System.getProperty("line.separator"));
          }
          in2.close();
        } catch (IOException ex){
          ex.printStackTrace();
        }
        JSONArray views = new JSONArray(contents2.toString());
%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"
         "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">

<html xmlns="http://www.w3.org/1999/xhtml">
<head>
<title>Hadoop Infrastructure Care Center</title>
<meta name="viewport" content="width=320; initial-scale=1.0; maximum-scale=1.0; user-scalable=0;"/>
<style type="text/css" media="screen">@import "css/iui.css";</style>
<script type="text/javascript" src="/hicc/js/workspace/prototype.js"></script>
<script type="text/javascript" src="/hicc/js/calendar.js"></script>
<script type="text/javascript" src="/hicc/js/timeframe.js"></script>
<script type="text/javascript" src="/hicc/js/lang/calendar-en.js"></script>
<script type="text/javascript" src="/hicc/js/calendar-setup.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/scriptaculous.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/effects.js"></script>
<script type="text/javascript" src="/hicc/js/workspace/dragdrop.js"></script>
<script type="text/javascript" src="/hicc/js/time.js"></script>
<script type="application/x-javascript" src="js/iui.js"></script>
<script type="application/x-javascript" src="js/workspace/workspace.js"></script>
<script type="text/javascript" src="/hicc/js/slider.js"></script>
<script type="text/javascript" src="/hicc/js/swfobject.js"></script>

<script type="application/x-javascript">
function render_widget(url) {
    myAjax=new Ajax.Request(
        url,
        {
            method: 'get',
            asynchronous: true,
            onSuccess: function(transport) {
                loadContentComplete(transport);
            },
            onFailure: function(error) {
                alert(error);
            },
        });
}
</script>
</head>

<body onclick="console.log('Hello', event.target);">
    <div class="toolbar">
        <h1 id="pageTitle"></h1>
        <a id="backButton" class="button" href="#"></a>
        <a class="button" href="#viewForm">Views</a>
    </div>
    <ul id="home" title="HICC" selected="true">
        <% for(int i=0;i<((JSONArray)view.get("pages")).length();i++) {
               String title = (String)((JSONObject)((JSONArray)view.get("pages")).get(i)).get("title"); %>
        <li><a href="#<%=i%>"><%= title %></a></li>
        <% } %>
    </ul>
    <% for(int i=0;i<((JSONArray)view.get("pages")).length();i++) {
           String title = (String)((JSONObject)((JSONArray)view.get("pages")).get(i)).get("title"); %>
    <ul id="<%= i %>" title="<%= title %>">
<%         JSONArray column = (JSONArray)((JSONObject)((JSONArray)view.get("pages")).get(i)).get("layout");
           for(int c=0;c<column.length();c++) {
               JSONArray widgets = (JSONArray)column.get(c);
               for(int j=0;j<widgets.length();j++) { 
                   String widget_title = (String)((JSONObject)widgets.get(j)).get("title"); 
                   String widget_url = (String)((JSONObject)widgets.get(j)).get("module");
                   JSONArray widget_param = (JSONArray)((JSONObject)widgets.get(j)).get("parameters");
                   String widget_parms = "";
                   for(int p=0;p<widget_param.length();p++) {
                       JSONObject parm = (JSONObject)widget_param.get(p);
                       if(parm.get("name").getClass()!=parm.get("value").getClass()) {
                           JSONArray values = (JSONArray)parm.get("value");
                           for(int v=0;v<values.length();v++) {
                               widget_parms+="&";
                               widget_parms+=(String)parm.get("name")+"=";
                               widget_parms+=URLEncoder.encode((String)values.get(v));
                           }
                       } else {
                           widget_parms+="&";
                           widget_parms+=(String)parm.get("name")+"=";
                           widget_parms+=URLEncoder.encode((String)parm.get("value"));
                       }
                   }
                   widget_parms+="&boxId=999_999";
%>
        <li><a href="#999_999" onclick="render_widget('<%=widget_url%>?<%=widget_parms%>');"><%= widget_title %></a></li>
           <%  }
           } %>
    </ul>
    <% } %>

                <ul id="viewForm" title="Views">
<% for(int v=0;v<views.length();v++) { %>
                    <li><a href="iphone.jsp?view=<%= ((JSONObject)views.get(v)).get("key") %>" target="_self"><%= ((JSONObject)views.get(v)).get("description") %></a></li>
<% } %>
                </ul>
<div id="999_999" title="Widget" class="panel">
<div id="dragableBoxContent999_999"></div>
<div id="dragableBoxHeader999_999"></div>
<div id="dragableBoxStatusBar999_999"></div>
</div>
</body>
</html>
