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
package org.apache.hadoop.chukwa.database;

import junit.framework.*;
import java.util.*;
import java.text.*;
import java.io.*;
import java.net.URL;
import java.sql.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.mortbay.jetty.Server;
import org.mortbay.xml.XmlConfiguration;
import org.apache.hadoop.chukwa.util.*;

/*
 * Testing the JSON output from the website with the database result.
 *
 */
public class TestDatabaseWebJson extends TestCase {
    protected HashMap testTables;
    protected String data_url="http://localhost:8080/hicc/jsp/get_db_data.jsp";
    private Server server = null;
    private Log log = LogFactory.getLog(TestDatabaseWebJson.class);

    /* 
     * setup list of tables to do testing. 
     * Add the table name and the table's primary keys.
     */
    protected void setUp() {
      testTables = new HashMap();

      ArrayList<String> keys = new ArrayList<String>();
      keys.add("timestamp");
      keys.add("mount");
      testTables.put("cluster_disk", keys);

      keys = new ArrayList<String>();
      keys.add("timestamp");
      testTables.put("cluster_system_metrics", keys);

      keys = new ArrayList<String>();
      keys.add("timestamp");
      keys.add("host");
      keys.add("mount");
      testTables.put("disk", keys);

      keys = new ArrayList<String>();
      keys.add("job_id");
      testTables.put("mr_job", keys);

      keys = new ArrayList<String>();
      keys.add("task_id");
      testTables.put("mr_task", keys);

      keys = new ArrayList<String>();
      keys.add("timestamp");
      testTables.put("system_metrics", keys);
      URL serverConf = TestDatabaseWebJson.class
        .getResource("/WEB-INF/jetty.xml");
      server = new Server();
      XmlConfiguration configuration;
      try {
        configuration = new XmlConfiguration(serverConf);
        configuration.configure(server);
        server.start();
        server.setStopAtShutdown(true);
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
      }
    }

    protected void tearDown() {
      try {
        server.stop();
        Thread.sleep(2000);
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
      }
    }

    /*
     * similar to PHP join function to join a string array to one single string.
     * for example, if the token is "," and the strings array is ('a','b','c')
     * the final result will be "a,b,c"
     *
     * @param token The separator which join the strings together
     * @param strings list of strings which we want to merge together
     * @return final string which merge together.
     */
    protected static String join( String token, ArrayList<String> strings )
    {
        StringBuffer sb = new StringBuffer();
	
        for( int x = 0; x < ( strings.size() - 1 ); x++ )  {
	    sb.append( strings.get(x) );
	    sb.append( token );
	}
        sb.append( strings.get( strings.size() - 1 ) );
	
        return( sb.toString() );
    }

    /*
     * format the query string for the database testing. Select the
     * primary key value from the json object.
     *
     * @param tableName The name of the database table to build the query on.
     * @param jo JSON object which contain the primary key of the row which we 
     *           want to test.
     * @return the actual database query string for the row selection.
     */
    protected String getDatabaseQuery(String tableName, JSONObject jo) {
	ArrayList<String> keys = (ArrayList<String>)testTables.get(tableName);
	ArrayList<String> criterias = new ArrayList<String>();
	Iterator i = keys.iterator();
	while (i.hasNext()) {
	    String key=(String)i.next();
	    try {
		String value=(String)jo.get(key);
		if (key.compareToIgnoreCase("timestamp")==0) {
		    value=DatabaseWriter.formatTimeStamp(Long.parseLong(value));
		}
		String c=key+"=\""+value+"\"";
		criterias.add(c);
	    } catch (Exception e) {
		System.out.println("Cannot get value for key: "+key);
	    }
	}
	String criteria=join(" and ", criterias);

	String query="select * from ["+tableName+"] where "+criteria;
	return query;
    }

    /*
     * the function will do the actual table verification. If will first
     * get the result from the website JSON object. Then it will compare the
     * JSON object with the values in the database.
     *
     * @param table name of the table to be verified.
     */
    protected void verifyTableData(String table) {
	Calendar startCalendar = new GregorianCalendar();
	// startCalendar.add(Calendar.HOUR_OF_DAY,-1);
	startCalendar.add(Calendar.MINUTE, -30);
	long startTime=startCalendar.getTime().getTime();

	Calendar endCalendar = new GregorianCalendar();
	// endCalendar.add(Calendar.HOUR_OF_DAY,1);
	long endTime=endCalendar.getTime().getTime();

	String url=data_url+"?table="+table+"&start="+startTime+"&end="+endTime;
	System.out.println(url);

	HttpClient client = new HttpClient();       
	GetMethod method = new GetMethod(url);


	try {

	    /*
	     * 1. get the json result for the specified table
	     */
	    int statusCode = client.executeMethod(method);
	    if (statusCode != HttpStatus.SC_OK) {
		System.out.println("Http Error: "+method.getStatusLine());
	    }
	    BufferedReader reader = new BufferedReader(new InputStreamReader( method.getResponseBodyAsStream(),
									      method.getResponseCharSet()));
	    String json_str="";
	    String str;
	    while ((str = reader.readLine()) != null) {
		json_str+=str;
	    }
	    
	    /*
	     * 2. convert the json string to individual field and compare it 
	     * with the database
	     */

	    String cluster = "demo";
	    DatabaseWriter db = new DatabaseWriter(cluster);

	    JSONArray json_array=(JSONArray)JSONValue.parse(json_str);
	    for (int i=0; i < json_array.size(); i++) {
		JSONObject row_obj=(JSONObject) json_array.get(i);

		// get the database row

		String queryString=getDatabaseQuery(table, row_obj);
		Macro m=new Macro(startTime, endTime, queryString);
		ResultSet rs = db.query(m.toString());
		// move to the first record
		rs.next();
		ResultSetMetaData md=rs.getMetaData();
		Iterator names=row_obj.keySet().iterator();
		while (names.hasNext()) {
		    String name=(String)names.next();
		    String jsonValue=(String)row_obj.get(name);
		    String dbValue=rs.getString(name);
		    int dbCol=rs.findColumn(name);
		    int dbType=md.getColumnType(dbCol);
		    if (dbType==93) {
			// timestamp
			dbValue=Long.toString(rs.getTimestamp(name).getTime());
		    }
		    // System.out.println("compare "+name+":"+dbType+":"+dbValue+":"+jsonValue);
		    assertEquals(dbValue, jsonValue);
		}
	    }

	    db.close();
	} catch (SQLException e) {
	    System.out.println("Exception: "+e.toString()+":"+e.getMessage());
	    System.out.println("Exception: "+e.toString()+":"+e.getSQLState());
	    System.out.println("Exception: "+e.toString()+":"+e.getErrorCode());
            fail("SQL Error:"+ExceptionUtil.getStackTrace(e));
	} catch (Exception eOther) {
	    System.out.println("Other Exception: "+eOther.toString());
	    eOther.printStackTrace();
            fail("Error:"+ExceptionUtil.getStackTrace(eOther));
	} finally {
	}
    }

    /*
     * Perform the actual testing. It will get the result from the web URL first.
     * Then it will get the result from the database and compare it.
     */
    public void testJsonResult() {
	Iterator i=testTables.keySet().iterator();
	while (i.hasNext()) {
	    String tableName=(String)i.next();
	    verifyTableData(tableName);

	}
    }
}
