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
<%

XssFilter xf = new XssFilter(request);

// decide type of statistics we want
String query_stats_mode = (String) xf.getParameter("heatmap_datanode_stattype");
if (query_stats_mode == null || query_stats_mode.length() <= 0) {
  query_stats_mode = new String("transaction_count");
}

// decide type of state we're interested in
String query_state = (String) xf.getParameter("heatmap_datanode_state");
if (query_state == null || query_state.length() <= 0) {
  query_state = new String("read_local");
}

HashMap<String, String> prettyStateNames = new HashMap<String, String>();

prettyStateNames.put("read_local", "Local Block Reads");
prettyStateNames.put("write_local", "Local Block Writes");
prettyStateNames.put("read_remote", "Remote Block Reads");
prettyStateNames.put("write_remote", "Remote Block Writes");
prettyStateNames.put("write_replicated", "Replicated Block Writes");

HashMap<String, String> prettyStatisticNames = new HashMap<String, String>();

prettyStatisticNames.put("transaction_count", "Number of Transactions");
prettyStatisticNames.put("avg_duration", "Average Duration<br />(ms)");
prettyStatisticNames.put("avg_volume", "Average Volume<br />(bytes)");
prettyStatisticNames.put("total_duration", "Total Duration<br />(ms)");
prettyStatisticNames.put("total_volume", "Total Volume<br />(bytes)");

%>
  <html><head> 

    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"> 
      <title>2D Spectrum Viewer</title> 
      <link href="/hicc/css/heatmap/layout.css" rel="stylesheet" type="text/css"> 
        <script language="javascript" type="text/javascript" src="/hicc/js/jquery-1.3.2.min.js"></script>
        <script language="javascript" type="text/javascript" src="/hicc/js/jquery.flot.pack.js"></script>

        <script language="javascript" type="text/javascript" src="/hicc/js/excanvas.pack.js"></script>
        <script id="source" language="javascript" type="text/javascript" src="heatmap-datanode-data-js.jsp?heatmap_datanode_stattype=<%= query_stats_mode %>&heatmap_datanode_state=<%= query_state %>"></script> 
        <script>
          function activateplot()
          {
            document.getElementById('clearSelection').click();
          }
        </script>
        <script id="source2" language="javascript" type="text/javascript">
// to eventually be moved out
// this takes the data structures from the data generation and
// generates data structures for flot to plot

          function d2h(d) {return (Math.round(d).toString(16));}

          // external vars: heatmap_size, heatmap_data, heatmap_names
          function generateGraph() 
          {
            var tmpstring = ' ';
            var count = 0;
            var minvalue = 0, maxvalue = 0;
            var COLOR_MAX = 255;
            var COLOR_MIN = 0;
            var SCALE=1;

            color_array = new Array(heatmap_size * heatmap_size);
            graph_data_array = new Array(heatmap_size * heatmap_size);
            graph_data_array_small = new Array(heatmap_size * heatmap_size);
            series_array = new Array(heatmap_size * heatmap_size);
            ticknames_array = new Array(heatmap_size);
            graph_tooltips = new Array(heatmap_size+1);
            
            var minstarted = 0;
            for (i = 0; i < heatmap_size; i++) {
              graph_tooltips[i+1] = new Array(heatmap_size+1);
              for (j = 0; j < heatmap_size; j++) {
                // determine min/max
                if (count <= 0) {
                  if (heatmap_data[count][2] > 0) {
                    minvalue = heatmap_data[count][2];
                    minstarted = 1;
                  }
                  maxvalue = heatmap_data[count][2];
                } else {
                  if (heatmap_data[count][2] > 0) {
                    if (minstarted > 0) {
                      minvalue = heatmap_data[count][2] > minvalue ? minvalue : heatmap_data[count][2];
                    } else {
                      minvalue = heatmap_data[count][2];
                      minstarted = 1;
                    }
                  }
                  maxvalue = heatmap_data[count][2] < maxvalue ? maxvalue : heatmap_data[count][2];
                }
                // create coordinates
                // graph_data_array[count] = {
                //   data: [i+1,j+1]
                // };
                // graph_data_array[count] = new Array(2);
                // graph_data_array[count][0] = i+1;
                // graph_data_array[count][1] = j+1;

                graph_tooltips[i+1][j+1] = 'State: <%= prettyStateNames.get(query_state) %><br />Statistic: <%= prettyStatisticNames.get(query_stats_mode) %><br />Value: ' 
                  + heatmap_data[count][2] + "<br />From: " + heatmap_names[i] + "<br />"
                  + "To: " + heatmap_names[j];
                  
                count++;                
              }
              ticknames_array[i] = [i+1, heatmap_names[i]];
            }
            
            $("#scale_max_placeholder").text(maxvalue);
            $("#scale_mid_placeholder").text(((maxvalue-minvalue)/2)+minvalue);
            $("#scale_min_placeholder").text(minvalue);
            
            var colorMap = new Array(7);
            colorMap[0]="0000ff";
            colorMap[1]="0099ff"; 
            colorMap[2]="00ffff";
            colorMap[3]="00ff00"; 
            colorMap[4]="ffff00";
            colorMap[5]="ff9900"; 
            colorMap[6]="ff0000";
            count = 0;
            for (i = 0; i < heatmap_size; i++) {
              for (j = 0; j < heatmap_size; j++) {
                var opacity=0.9;
                if (heatmap_data[count][2] == 0) {
                  colorstring = '000099';
                  opacity=0;
                } else {
                  opacity=0.9;
                  var index = Math.round((heatmap_data[count][2] - minvalue) / (maxvalue - minvalue) * 7);
                  if(index>6) {
                    index=6;
                  } else if(index<0) {
                    index=0;
                  }
                  if(index==6) {
                    opacity=0.9;
                  } else if(index==5) {
                    opacity=0.8;
                  } else if(index==4) {
                    opacity=0.7;
                  } else if(index==3) {
                    opacity=0.6;
                  } else if(index==2) {
                    opacity=0.5;
                  } else if(index==1) {
                    opacity=0.4;
                  } else if(index==0) {
                    opacity=0.3;
                  }
                  colorstring = colorMap[index];
                }
                
                colorstring = '#' + colorstring;
                color_array[count] = colorstring;
                series_array[count] = { lines: {show: true, radius:999} };

                graph_data_array[count] = {
                  points: {show: true, radius: 20*opacity, lineWidth: 0, fill: opacity, fillColor: false }, 
                  color: colorstring,
                  data: [[(heatmap_data[count][0]+1)/SCALE, (heatmap_data[count][1]+1)/SCALE]]
                }
                graph_data_array_small[count] = {
                  points: {show: true, radius: 4*opacity, lineWidth: 0, fill: opacity, fillColor: false}, 
                  color: colorstring,
                  data: [[(heatmap_data[count][0]+1)/SCALE, (heatmap_data[count][1]+1)/SCALE]]
                }

                count++;
              }
            }

            graph_options = {
              grid: { hoverable: true, backgroundColor: '#000099' },
              yaxis: {autoscaleMargin: 0.1, ticks: [] },
              xaxis: {autoscaleMargin: 0.1, ticks: [] },
              selection: { mode: "xy" },
              shadowSize: 0
            };
            graph_options_small = {
              grid: { hoverable: true, backgroundColor: '#000099' },
              yaxis: {autoscaleMargin: 0.1, ticks: [] },
              xaxis: {autoscaleMargin: 0.1, ticks: [] },
              selection: { mode: "xy" },
              shadowSize: 0
            };            
          }
        </script>
        <script id="source3" language="javascript" type="text/javascript">
// to eventually be moved out
// this generates the actual flot options

function plotGraph() {

var placeholder = $("#placeholder");
var plot = $.plot(placeholder, graph_data_array, graph_options);

var smallplotplaceholder = $("#smallplotplaceholder");
var smallplot = $.plot(smallplotplaceholder, graph_data_array_small, graph_options_small);

placeholder.bind("plotselected", function (event, ranges) {
  if (ranges.xaxis.to - ranges.xaxis.from < 0.00001)
    ranges.xaxis.to = ranges.xaxis.from + 0.00001;
  if (ranges.yaxis.to - ranges.yaxis.from < 0.00001)
    ranges.yaxis.to = ranges.yaxis.from + 0.00001;

  plot = $.plot(placeholder, graph_data_array,
    $.extend(true, {}, graph_options, {
    grid: { hoverable: true },
    xaxis: { min: ranges.xaxis.from, max: ranges.xaxis.to }, 
    yaxis: { min: ranges.yaxis.from, max: ranges.yaxis.to }
    }
  )
);

// don't fire event on the overview to prevent eternal loop
smallplot.setSelection(ranges, true);

});

smallplotplaceholder.bind("plotselected", function (event, ranges) {
  plot.setSelection(ranges);
});



// Hover text
var previousPoint = null;
$("#placeholder").bind("plothover", function (event, pos, item) {
  if (item) {
    if (previousPoint != item.datapoint) {
      previousPoint = item.datapoint;

      $("#tooltip").remove();
      var x = item.datapoint[0].toFixed(2),
      y = item.datapoint[1].toFixed(2);

      showTooltip(item.pageX, item.pageY, lookupStateInfo(item.datapoint[0], item.datapoint[1]));

    } else {
      $("#tooltip").remove();
      previousPoint = null;            
    }
  }
});       

}

function lookupStateInfo(x,y) {
  return graph_tooltips[x][y];
}

function showTooltip(x, y, contents) {
  $('<div id="tooltip">' + contents + '</div>').css( {
    position: 'absolute',
    display: 'none',
    top: y + 5,
    left: x + 5,
    border: '1px solid #fdd',
    padding: '2px',
    'background-color': '#fee',
    opacity: 0.80
  }).appendTo("body").fadeIn(200);
}

        </script>
      </head><div FirebugVersion="1.3.3" style="display: none;" id="_firebugConsole"></div><body onload="generateData(); generateGraph(); plotGraph();">
      <table cellpadding="0" cellspacing="0">
        <tbody>
          <tr>

            <td align="right" valign="top" rowspan="1"><div id="placeholder" style="width: 400px; height: 400px; position: relative;"><canvas width="400" height="400"></canvas><canvas style="position: absolute; left: 0px; top: 0px;" width="400" height="400"></canvas></div></td>

            <td rowspan="1"><div style="width:10px">&nbsp;</div></td>

            <td align="middle"><div id="smallplotplaceholder", style="width:100px;height:100px;"><canvas style="position: absolute; left: 0px; top: 0px;" width="100" height="100"></canvas></div>
            
              <br />
              
              State: <b><%= prettyStateNames.get(query_state) %></b><br />
              Statistic: <b><%= prettyStatisticNames.get(query_stats_mode) %></b>
            
            </td>

          </tr>

          <tr>
            <td colspan="3" align="middle" valign="top">

              <br />

              <span id="resultcountholder">No results returned. </span>

              <br />

              <table cellpadding="0" cellspacing="4"><tbody>
                <tr><th colspan="2">Scale</th></tr>
                <tr>
                  <td bgcolor="#ff0000">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#ff9900">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#ffff00">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#00ff00">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#00ffff">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#0099ff">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#0000ff">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                  <td bgcolor="#000099">&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;</td>
                </tr>
                
                <tr>
                  <td><span id="scale_max_placeholder"></span></td>
                  <td>&nbsp;</td>
                  <td>&nbsp;</td>
                  <td><span id="scale_mid_placeholder"></span></td>
                  <td>&nbsp;</td>
                  <td>&nbsp;</td>
                  <td><span id="scale_min_placeholder"></span></td>
                  <td>0</td>
                </tr>
              </tbody></table>
            </td>
          </tr>

        </tbody></table> 
      </body></html>
