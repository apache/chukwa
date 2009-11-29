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
<%@ page import = "java.util.Calendar, java.util.Date, java.sql.*, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*,java.lang.Math, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.lang.StringBuilder, org.apache.hadoop.chukwa.util.XssFilter" %>
  <% response.setContentType("text/html"); %>
  <html><head> 

    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"> 
      <title>2D Spectrum Viewer</title> 
      <link href="/hicc/css/heatmap/layout.css" rel="stylesheet" type="text/css"> 
        <script language="javascript" type="text/javascript" src="/hicc/js/jquery-1.3.2.min.js"></script>
        <script language="javascript" type="text/javascript" src="/hicc/js/jquery.flot.pack.js"></script>

        <script language="javascript" type="text/javascript" src="/hicc/js/excanvas.pack.js"></script>
        <script>
          function activateplot()
          {
          document.getElementById('clearSelection').click();
          }
        </script>
        <script id="source" language="javascript" type="text/javascript" src="swimlanes-event-data-js.jsp"> 
        </script> 
      </head><div FirebugVersion="1.3.3" style="display: none;" id="_firebugConsole"></div><body onload="activateplot()">
      <table cellpadding="0" cellspacing="0">
        <tbody>
          <tr>

            <td align="right" valign="top" rowspan="2"><div id="placeholder" style="width: 600px; height: 400px; position: relative;"><canvas width="600" height="400"></canvas><canvas style="position: absolute; left: 0px; top: 0px;" width="600" height="400"></canvas></div></td>

            <td rowspan="2"><div style="width:10px">&nbsp;</div></td>

            <td><div id="smallplotplaceholder", style="width:166px;height:100px;"><canvas style="position: absolute; left: 0px; top: 0px;" width="166" height="100"></canvas></div></td>

          </tr>

          <tr>
            <td align="right">

              <span id="resultcountholder">No results returned. </span>

              <br />

              <table cellpadding="0" cellspacing="2"><tbody>
                <tr><th colspan="2">Legend</th></tr>
                <tr><td bgcolor="#0000cc">&nbsp;&nbsp;</td><td>Map</td></tr>
                <tr><td bgcolor="#00cc00">&nbsp;&nbsp;</td><td>Reduce</td></tr>
                <tr><td bgcolor="#ff0000">&nbsp;&nbsp;</td><td>Reduce-ShuffleWait</td></tr>
                <tr><td bgcolor="#330000">&nbsp;&nbsp;</td><td>Reduce-Sort</td></tr>
                <tr><td bgcolor="#990000">&nbsp;&nbsp;</td><td>Reduce-Reducer</td></tr>
                <tr><td bgcolor="#333333">&nbsp;&nbsp;</td><td>Other</td></tr>
              </tbody></table>
            </td>
          </tr>

        </tbody></table> 
      </body></html>
