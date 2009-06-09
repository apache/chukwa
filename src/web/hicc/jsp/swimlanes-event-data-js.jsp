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
<%@ page import = "java.util.Calendar, java.util.Date, java.sql.*, java.text.SimpleDateFormat, java.util.*, java.sql.*,java.io.*,java.lang.Math, java.util.Calendar, java.util.Date, java.text.SimpleDateFormat, java.lang.StringBuilder, org.apache.hadoop.chukwa.hicc.ClusterConfig, org.apache.hadoop.chukwa.hicc.TimeHandler, org.apache.hadoop.chukwa.util.DatabaseWriter, org.apache.hadoop.chukwa.database.Macro, org.apache.hadoop.chukwa.database.DatabaseConfig, org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page session="true" %> 
<%
HashMap<String, String> state_colours = new HashMap<String, String>();
state_colours.put("map","#0000cc");
state_colours.put("reduce","#00cc00");
state_colours.put("reduce_reducer", "#ff0000");
state_colours.put("reduce_shufflewait", "#330000");
state_colours.put("reduce_sort", "#990000");
String defaultcolour = new String("#333333");

response.setContentType("text/javascript");

XssFilter xf = new XssFilter(request);
TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
long start = time.getStartTime();
long end = time.getEndTime();
String cluster = (String) session.getAttribute("cluster");
String table = "mapreduce_fsm";

if(xf.getParameter("event_type")!=null) {
  table = xf.getParameter("event_type");
}
String query = "select job_id,friendly_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname from ["+table+"] where finish_time between '[start]' and '[end]'";
Macro mp = new Macro(start,end,query, request);
query = mp.toString() + " order by start_time";

out.print("/" + "/" + "swimlanes: " + mp.toString() + " cluster: " + cluster);

ArrayList<HashMap<String, Object>> events = new ArrayList<HashMap<String, Object>>();

Connection conn = null;
Statement stmt = null;
ResultSet rs = null;

DatabaseWriter dbw = new DatabaseWriter(cluster);
try {
  rs = dbw.query(query);
  ResultSetMetaData rmeta = rs.getMetaData();
  int col = rmeta.getColumnCount();
  while (rs.next()) {
    HashMap<String, Object> event = new HashMap<String, Object>();
    long event_time=0;
    for(int i=1;i<=col;i++) {
      if(rmeta.getColumnType(i)==java.sql.Types.TIMESTAMP) {
        event.put(rmeta.getColumnName(i),rs.getTimestamp(i).getTime());
      } else {
        event.put(rmeta.getColumnName(i),rs.getString(i));
      }
    }
    events.add(event);
  }
} catch (SQLException ex) {
  // handle any errors
  //out.println("SQLException: " + ex.getMessage());
  //out.println("SQLState: " + ex.getSQLState());
  //out.println("VendorError: " + ex.getErrorCode());
} finally {
  // it is a good idea to release
  // resources in a finally{} block
  // in reverse-order of their creation
  // if they are no-longer needed
  dbw.close();
}
%>
$(function () {
<%
  SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
  HashMap<String, Integer> reduce_ytick_ids = new HashMap<String, Integer>();

  StringBuilder opts_colour_string, opts_series_string, opts_data_string;
  opts_colour_string = new StringBuilder();
  opts_series_string = new StringBuilder();
  opts_data_string = new StringBuilder("[");

  StringBuilder withstart_points_series_string = new StringBuilder();
  StringBuilder nostart_points_series_string = new StringBuilder();
  StringBuilder start_points_data_string = new StringBuilder();
  StringBuilder start_points_colour_string = new StringBuilder();

  StringBuilder lookupFunctionString = new StringBuilder();

  out.println("/" + "/" + events.size() + " results returned.");
  int unique_plot_yticks = 0;
  int global_ycounter = 1;

%> $("#resultcountholder").text("<%= events.size() %> states returned.");
<%

  int start_millis = 0, end_millis = 0;
  for(int i=0;i<events.size();i++) {
    HashMap<String, Object> event = events.get(i);
    start=(Long)event.get("start_time");
    end=(Long)event.get("finish_time");
    String event_time = format.format(start);
    String launch_time = format.format(event.get("start_time"));
    String event_end_time = format.format(end);
    String cell = (String) event.get("state_name");
    start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
    end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));

    String colourstring = null;
    if (state_colours.containsKey(cell)) {
      colourstring = state_colours.get(cell);
    } else {
      colourstring = defaultcolour;
    }
    %>// <%= event.get("friendly_id") %>
<%
  float ytick;
  if (cell.startsWith("reduce")) { 
    Integer thenum = null;
    thenum = reduce_ytick_ids.get((String)event.get("friendly_id"));
    if (thenum != null) {
      ytick = thenum.intValue();%> // old
<%      
    } else {
      ytick = global_ycounter;
      reduce_ytick_ids.put((String)event.get("friendly_id"),new Integer(global_ycounter));
      global_ycounter++; %> // new
<%
    } 
    if (cell.endsWith("reducer")) {
      ytick += 0.75;
    } else if (cell.endsWith("sort")) {
      ytick += 0.5;
    } else if (cell.endsWith("shufflewait")) {
      ytick += 0.25;
    }
%> var d<%= i %> = [[<%= start %>,<%= ytick %>],[<%= end %>,<%= ytick %>]];
<%   
  } else { 
    ytick = global_ycounter;
%> var d<%= i %> = [[<%= start %>,<%= global_ycounter %>],[<%= end %>,<%= global_ycounter %>]];
<%
    global_ycounter++;
  }

%> var ds<%= i %> = [[<%= start %>,<%= ytick %>]];
<%
  start_points_data_string.append(",");
  start_points_data_string.append("ds");
  start_points_data_string.append(i);
  
  lookupFunctionString.append("if (y == ");
  lookupFunctionString.append(ytick);
  lookupFunctionString.append(" ) {\n");
  lookupFunctionString.append("return '");
  lookupFunctionString.append("State: ");
  lookupFunctionString.append(event.get("state_name"));
  lookupFunctionString.append(" / Status: ");
  lookupFunctionString.append(event.get("status"));
  lookupFunctionString.append(" <br />");
  lookupFunctionString.append("Host: ");
  lookupFunctionString.append(event.get("hostname"));
  lookupFunctionString.append(" <br />");
  lookupFunctionString.append("Duration: ");
  lookupFunctionString.append((end-start)+(end_millis-start_millis));
  lookupFunctionString.append("ms <br />");
  lookupFunctionString.append("[");
  lookupFunctionString.append(event.get("friendly_id"));
  lookupFunctionString.append("]");
  lookupFunctionString.append("';\n");
  lookupFunctionString.append("}\n");

  opts_series_string.append("lines: { show:true }, \n");
  withstart_points_series_string.append("points: {show:true}, \n");
  nostart_points_series_string.append("points: {show:false}, \n");
  if (i > 0) opts_colour_string.append(", ");
  opts_colour_string.append("\"");
  opts_colour_string.append(colourstring);
  opts_colour_string.append("\"");
  start_points_colour_string.append(",");
  start_points_colour_string.append("\"");
  start_points_colour_string.append(colourstring);
  start_points_colour_string.append("\"");
  if (i > 0) opts_data_string.append(", ");
  opts_data_string.append("d");
  opts_data_string.append(i);     
} 

StringBuilder opts_data_withstart_string = new StringBuilder(opts_data_string.toString());
opts_data_withstart_string.append(start_points_data_string.toString());
opts_data_string.append("];");
opts_data_withstart_string.append("];");
%>
var data = <%= opts_data_string.toString() %>
var data_withstart = <%= opts_data_withstart_string.toString() %>
var options_withstart = {
  legend: { show: true, container: $("#legend") },
  colors: [ <%= opts_colour_string.toString() %>],
  shadowSize: 0, 
  xaxis: { mode: "time", timeformat: "%H%M.%S" }, 
  yaxis: { autoscaleMargin: 0 },
  <%= opts_series_string.toString() %> 
  <%= withstart_points_series_string.toString() %> selection: { mode: "xy" }
};

var options_small = {
  legend: { show: true, container: $("#legend") },
  colors: [ <%= opts_colour_string.toString() %> ], 
  shadowSize: 0, 
  xaxis: { mode: "time", timeformat: "%H:%M" }, 
  yaxis: { autoscaleMargin: 0 },
  <%= opts_series_string.toString() %> 
  <%= nostart_points_series_string.toString() %> selection: { mode: "xy" }
};


var placeholder = $("#placeholder");
var plot = $.plot(placeholder, data_withstart, options_withstart);

var smallplotplaceholder = $("#smallplotplaceholder");
var smallplot = $.plot(smallplotplaceholder, data_withstart, options_small);

placeholder.bind("plotselected", function (event, ranges) {
  if (ranges.xaxis.to - ranges.xaxis.from < 0.00001)
    ranges.xaxis.to = ranges.xaxis.from + 0.00001;
  if (ranges.yaxis.to - ranges.yaxis.from < 0.00001)
    ranges.yaxis.to = ranges.yaxis.from + 0.00001;

  plot = $.plot(placeholder, data_withstart,
    $.extend(true, {}, options_withstart, {
    grid: { hoverable: true },
    xaxis: { min: ranges.xaxis.from, max: ranges.xaxis.to }, 
    yaxis: { min: ranges.yaxis.from, max: ranges.yaxis.to },
    }
  )
);

// don't fire event on the overview to prevent eternal loop
smallplot.setSelection(ranges, true);

});

smallplotplaceholder.bind("plotselected", function (event, ranges) {
  plot.setSelection(ranges);
});

function lookupStateInfo(y) {
  <%= lookupFunctionString %>
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


// Hover text
var previousPoint = null;
$("#placeholder").bind("plothover", function (event, pos, item) {
  if (item) {
    if (previousPoint != item.datapoint) {
      previousPoint = item.datapoint;

      $("#tooltip").remove();
      var x = item.datapoint[0].toFixed(2),
      y = item.datapoint[1].toFixed(2);

      showTooltip(item.pageX, item.pageY, lookupStateInfo(item.datapoint[1]));

    } else {
      $("#tooltip").remove();
      previousPoint = null;            
    }
  }
});       

$("#clearSelection").click(function () {
    plot.clearSelection();
  }
);   

});
