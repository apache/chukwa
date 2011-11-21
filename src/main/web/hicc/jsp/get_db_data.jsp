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
<%@ page import = "java.text.DecimalFormat" %>
<%@ page import = "java.text.NumberFormat" %>
<%@ page import = "java.sql.*" %>
<%@ page import = "java.io.*" %>
<%@ page import = "org.json.*" %>
<%@ page import = "java.util.Calendar" %>
<%@ page import = "java.util.Date" %>
<%@ page import = "java.text.SimpleDateFormat" %>
<%@ page import = "java.util.*" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.ClusterConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.TimeHandler" %>
<%@ page import = "org.apache.hadoop.chukwa.util.DatabaseWriter" %>
<%@ page import = "org.apache.hadoop.chukwa.database.Macro" %>
<%@ page import = "org.apache.hadoop.chukwa.util.XssFilter" %>
<%@ page import = "org.apache.hadoop.chukwa.database.DatabaseConfig" %>
<%@ page import = "org.apache.hadoop.chukwa.hicc.DatasetMapper"  %> 
<%
    /* get the passed in parameters */
    String table=request.getParameter("table");
    String startDate=request.getParameter("start"); 	// in long format
    String endDate=request.getParameter("end");		// in long format


    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date startDateObj=new Date(Long.parseLong(startDate));
    String startDateString = sdf.format(startDateObj);

    Date endDateObj=new Date(Long.parseLong(endDate));
    String endDateString = sdf.format(endDateObj);

    /* get the query result */

    /* handle special case for other tables: mr_job, mr_task */
    String timeField="timestamp";
    if (table.compareToIgnoreCase("mr_job")==0) {
	timeField="finish_time";
    } else if (table.compareToIgnoreCase("mr_task")==0) {
	timeField="finish_time";
    } 
    String query="select * from ["+table+"] where "+timeField+" between '"
	+startDateString+"' and '"+endDateString+"'";
	
    Macro m=new Macro(Long.parseLong(startDate),Long.parseLong(endDate),query);

    String cluster="demo";
    DatabaseWriter db = new DatabaseWriter(cluster);

    String jsonString="[]";

    try {
	// first get the table
	DatabaseMetaData meta = db.getConnection().getMetaData();
	String tableName = m.computeMacro(table);

	ResultSet rsColumns = meta.getColumns(null, null, tableName, null);
    	ResultSet rs = db.query(m.toString());

	JSONArray ja = new JSONArray();

	while (rs.next()) {
	    rsColumns.beforeFirst();
	    JSONObject jo = new JSONObject();
	    while (rsColumns.next()) {
		String columnName=rsColumns.getString("COLUMN_NAME");
		String data_type=rsColumns.getString("TYPE_NAME");
	        String value="";
		if (data_type.compareToIgnoreCase("timestamp")==0) {
		    java.sql.Timestamp ts=rs.getTimestamp(columnName);
		    value=Long.toString(ts.getTime());
		} else {	
		    value=rs.getString(columnName);
		}
	 	//out.println(columnName+":"+value+"<br/>");
		jo.put(columnName, value);
	    }
	    ja.put(jo);
	}
	db.close();

	/* convert the result set to JSON */
	jsonString=ja.toString();

    } catch (Exception e) {
	// cannot execute the query
    }

    out.println(jsonString);

%>
