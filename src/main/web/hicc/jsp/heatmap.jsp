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
<%@ page import = "java.util.Hashtable, java.util.Enumeration, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, org.apache.hadoop.chukwa.hicc.TimeHandler, java.text.NumberFormat, org.apache.hadoop.chukwa.util.XssFilter" %>
<%
   XssFilter xf = new XssFilter(request);
   response.setContentType("text/html; chartset=UTF-8//IGNORE");
   response.setHeader("boxId", xf.getParameter("boxId"));

   String width = "600";
   if(xf.getParameter("width")!=null) {
     width=xf.getParameter("width");
   }

   String height = "400";
   if(xf.getParameter("height")!=null) {
     height=xf.getParameter("height");
   }

   String yLabel = "device";
   if(xf.getParameter("yLabel")!=null) {
     yLabel=xf.getParameter("yLabel");
   }

   String url = "/hicc/v1/heatmap/SystemMetrics/cpu/combined.?max=100";
   if(xf.getParameter("url")!=null) {
     url=xf.getParameter("url");
   }
%>
<!DOCTYPE html>
<html lang="en">
  <head>
    <style>

      #heatmapArea {
        display: block;
        position:absolute;
        float:left;
        width: <%= width %>px;
        height: <%= height %>px;
        top:0;
        left: 50px;
      }

      #yaxis {
        text-align: center;
        width: 50px;
        height: <%= height %>px;
        line-height: 400px;
      }

      p {
        border:0px solid red;
        writing-mode:lr-tb;
        -webkit-transform:rotate(270deg);
        -moz-transform:rotate(270deg);
        -o-transform: rotate(270deg);
        white-space:nowrap;
        bottom:0;
      }

      #xaxis {
        width: <%= width %>px;
        position: absolute;
        left: 0px;
        bottom: 10px;
        height: 20px;
        text-align: center;
        display: block;
      }

      body {
        color:#333;
        font-family: Oswald, Helvetica, Arial;
        font-weight:normal;
      }

    </style>
    <link href="/hicc/css/default.css" rel="stylesheet" type="text/css">
  </head>
  <body>
    <div id="yaxis">
      <p id="yLabel"></p>
    </div>
    <div id="heatmapArea"></div>
    <div id="xaxis">Time</div>
    <script src="/hicc/js/jquery-1.3.2.min.js" type="text/javascript" charset="utf-8"></script>
    <script type="text/javascript" src="/hicc/js/heatmap.js"></script>
    <script type="text/javascript">
      window.onload = function() {
        $.ajax({ 
          url: "<%= url %>", 
          dataType: "json", 
          success: function(data) {
            $('#yLabel').html(data.series + " <%= yLabel %>(s)");
            var config = {
              element: document.getElementById("heatmapArea"),
              radius: data.radius/2,
              opacity: 50,
              legend: {
                position: 'br',
                title: '<%= xf.getParameter("title") %> Distribution'
              }
            };
            var heatmap = h337.create(config);
            heatmap.store.setDataSet(data);
          }
        });
      };
    </script>
  </body>
</html>
