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

// TODO: Read parameters from query string or environment to choose mode
// of data desired, e.g. duration, # transactions, etc. ; 
// use widget properties to create selectors for these various modes

response.setContentType("text/javascript");

// initial setup
XssFilter xf = new XssFilter(request);
TimeHandler time = new TimeHandler(request, (String)session.getAttribute("time_zone"));
long start = time.getStartTime();
long end = time.getEndTime();
String cluster = (String) session.getAttribute("cluster");
String table = "filesystem_fsm";

// decide type of statistics we want
String query_stats_mode = (String) xf.getParameter("heatmap_datanode_stattype");
if (query_stats_mode == null || query_stats_mode.length() <= 0) {
  query_stats_mode = new String("transaction_count");
}

// decide type of state we're interested in
String query_state = (String) xf.getParameter("heatmap_datanode_state");
out.println("/" + "/ " + "state: " + query_state);
if (query_state == null || query_state.length() <= 0) {
  query_state = new String("read_local");
}

// actual work: process query
if(xf.getParameter("event_type")!=null) {
  table = xf.getParameter("event_type");
}
String query = "select block_id,start_time,finish_time,start_time_millis,finish_time_millis,status,state_name,hostname,other_host,bytes from ["+table+"] where finish_time between '[start]' and '[end]' and state_name like '" + query_state + "'";
Macro mp = new Macro(start,end,query, request);
query = mp.toString() + " order by start_time";

out.println("/" + "/" + "datanode: " + query + " cluster: " + cluster);

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

function generateData() {
<%
  SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
  HashMap<String, Integer> reduce_ytick_ids = new HashMap<String, Integer>();

  out.println("/" + "/ " + events.size() + " results returned.");

  HashSet<String> host_set = new HashSet<String>();
  HashMap<String, Integer> host_indices = new HashMap<String, Integer>();

  // collect hosts
  for(int i = 0; i < events.size(); i++) {
    HashMap<String, Object> event = events.get(i);
    String curr_host = (String) event.get("hostname");
    String other_host = (String) event.get("other_host");
    host_set.add(curr_host);
    host_set.add(other_host);
  }
  int num_hosts = host_set.size();
  
  Iterator<String> host_iter = host_set.iterator();
  for (int i = 0; i < num_hosts && host_iter.hasNext(); i++) {
    String curr_host = host_iter.next();
    host_indices.put(curr_host, new Integer(i));
  }
  
  out.println("/" + "/" + " Number of hosts: " + num_hosts);
  long stats[][] = new long[num_hosts][num_hosts];
  long count[][] = new long[num_hosts][num_hosts]; // used for averaging

  int start_millis = 0, end_millis = 0;

  // deliberate design choice to duplicate code PER possible operation
  // otherwise we have to do the mode check N times, for N states returned
  if (query_stats_mode.equals("transaction_count")) {
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("start_time");
      end=(Long)event.get("finish_time");
      start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
      end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
      String cell = (String) event.get("state_name");      
      String this_host = (String) event.get("hostname");
      String other_host = (String) event.get("other_host");
      int this_host_idx = host_indices.get(this_host).intValue();
      int other_host_idx = host_indices.get(other_host).intValue();
      
      // from, to
      stats[this_host_idx][other_host_idx] += 1;
    }
  } else if (query_stats_mode.equals("avg_duration")) {
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("start_time");
      end=(Long)event.get("finish_time");
      start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
      end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
      String cell = (String) event.get("state_name");      
      String this_host = (String) event.get("hostname");
      String other_host = (String) event.get("other_host");
      int this_host_idx = host_indices.get(this_host).intValue();
      int other_host_idx = host_indices.get(other_host).intValue();
      
      long curr_val = end_millis - start_millis + ((end - start)*1000);
      
      // from, to
      stats[this_host_idx][other_host_idx] += curr_val;
      count[this_host_idx][other_host_idx] += 1;
    }    
    for (int i = 0; i < num_hosts; i++) {
      for (int j = 0; j < num_hosts; j++) {
        if (count[i][j] > 0) stats[i][j] = stats[i][j] / count[i][j];
      }
    }
  } else if (query_stats_mode.equals("avg_volume")) {
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("start_time");
      end=(Long)event.get("finish_time");
      start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
      end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
      String cell = (String) event.get("state_name");      
      String this_host = (String) event.get("hostname");
      String other_host = (String) event.get("other_host");
      int this_host_idx = host_indices.get(this_host).intValue();
      int other_host_idx = host_indices.get(other_host).intValue();
      
      long curr_val = Long.parseLong((String)event.get("bytes"));
      
      // from, to
      stats[this_host_idx][other_host_idx] += curr_val;
      count[this_host_idx][other_host_idx] += 1;
    }    
    for (int i = 0; i < num_hosts; i++) {
      for (int j = 0; j < num_hosts; j++) {
        if (count[i][j] > 0) stats[i][j] = stats[i][j] / count[i][j];
      }
    }
  } else if (query_stats_mode.equals("total_duration")) {
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("start_time");
      end=(Long)event.get("finish_time");
      start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
      end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
      String cell = (String) event.get("state_name");      
      String this_host = (String) event.get("hostname");
      String other_host = (String) event.get("other_host");
      int this_host_idx = host_indices.get(this_host).intValue();
      int other_host_idx = host_indices.get(other_host).intValue();
      
      double curr_val = end_millis - start_millis + ((end - start)*1000);
      
      // from, to
      stats[this_host_idx][other_host_idx] += curr_val;
    } 
  } else if (query_stats_mode.equals("total_volume")) {
    for(int i=0;i<events.size();i++) {
      HashMap<String, Object> event = events.get(i);
      start=(Long)event.get("start_time");
      end=(Long)event.get("finish_time");
      start_millis = Integer.parseInt(((String)event.get("start_time_millis")));
      end_millis = Integer.parseInt(((String)event.get("finish_time_millis")));      
      String cell = (String) event.get("state_name");      
      String this_host = (String) event.get("hostname");
      String other_host = (String) event.get("other_host");
      int this_host_idx = host_indices.get(this_host).intValue();
      int other_host_idx = host_indices.get(other_host).intValue();
      
      long curr_val = Long.parseLong((String)event.get("bytes"));
      
      // from, to
      stats[this_host_idx][other_host_idx] += curr_val;
    }    
  }
  
%>
heatmap_size = <%= num_hosts %>;
heatmap_data = [<%
  for (int i = 0; i < num_hosts; i++) {
    for (int j = 0; j < num_hosts; j++) {
      if (i > 0 || j > 0) out.println(",");
      out.print("[" + i + "," + j + "," + stats[j][i] + "]");
    }
  }
%>];
heatmap_names = [<%
  host_iter = host_set.iterator();
  for (int i = 0; i < num_hosts && host_iter.hasNext(); i++) {
    if (i > 0) out.print(",");
    out.print("'" + host_iter.next() + "'");
  }
%>];

 $("#resultcountholder").text("<%= events.size() %> states returned.");

}

