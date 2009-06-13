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

package org.apache.hadoop.chukwa.rest;

import java.util.*;
import java.sql.*;
import java.text.SimpleDateFormat;
import org.apache.commons.logging.*;
import org.apache.hadoop.chukwa.database.*;
import org.apache.hadoop.chukwa.util.*;
import org.apache.hadoop.chukwa.database.DatabaseSetup;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.rest.actions.*;
import org.apache.hadoop.chukwa.rest.objects.*;
import org.apache.hadoop.chukwa.rest.services.*;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import junit.framework.TestCase;

import com.sun.jersey.api.client.*;
import com.sun.jersey.api.client.filter.*;
import com.sun.jersey.spi.container.servlet.ServletContainer;

public class DatabaseRestServerSetup extends TestCase {
  public static String DATE_FORMAT_NOW="yyy-MM-dd HH:mm:ss";
  public Timestamp time;
  public String tableName;
  public String dateString;
  public String insertString;
  public String deleteString;
  WebResource resource;
  Client client;
  public DatabaseSetup dbSetup = new DatabaseSetup();
  int THREADS = 2;
  int restPort = 8080;
  private Server jettyServer = null;

  public DatabaseRestServerSetup() {
	  Calendar calendar = Calendar.getInstance();
	  this.time = new Timestamp(calendar.getTimeInMillis());
	  SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
	  this.dateString = sdf.format(this.time.getTime());
	  client = Client.create();
	  resource = client.resource("http://localhost:"+restPort+"/chukwa/v1/");
	  resource.addFilter(new LoggingFilter());
	  System.err.println("done logging filter");
  }

  protected String getCluster() {
	  // insert record into database	
	  String cluster=System.getProperty("CLUSTER", "");
	  if (cluster.compareTo("")==0)  {
	    ClusterConfig cc=new ClusterConfig();
	    Iterator<String> keys = cc.getClusters();
	    if (keys.hasNext()) {
		    cluster=keys.next();
	    }
	    System.setProperty("CLUSTER",cluster);
	  }

    if (cluster == null) {
	    cluster = "demo";
	  }

	  return cluster;
  }

  protected void setUp() {
	  dbSetup.setUpDatabase();
	  String cluster = getCluster();
	  DatabaseWriter dbw = new DatabaseWriter(cluster);
	  long startTime = this.time.getTime();
	  long endTime = this.time.getTime();

	  String query=this.insertString;
	  Macro mp = new Macro(startTime, endTime, query, null);
	  query = mp.toString();
	  try {
	    dbw.execute(query);
	  } catch (Exception e) {
	    System.err.println(e.toString());
	  }
    SelectChannelConnector jettyConnector = new SelectChannelConnector();
    jettyConnector.setLowResourcesConnections(THREADS-1);
    jettyConnector.setLowResourceMaxIdleTime(1500);
    jettyConnector.setPort(restPort);
    
    ServletHolder sh = new ServletHolder(ServletContainer.class);
    sh.setInitParameter("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.PackagesResourceConfig");
    sh.setInitParameter("com.sun.jersey.config.property.packages", "org.apache.hadoop.chukwa.rest.actions");
    
    // Set up jetty server proper, using connector
    jettyServer = new Server(restPort);
    Context root = new Context(jettyServer, "/", Context.SESSIONS);
    root.addServlet(sh, "/chukwa/v1/*");
    try {
    jettyServer.start();
    jettyServer.setStopAtShutdown(true);
    } catch(Throwable e) {
      fail("Fail to start chukwa REST API");
    }
  }

  protected void tearDown() {
    try {
      jettyServer.stop();
    } catch (Exception e) {
      fail("Fail to stop chukwa REST API");
    }
  }

  // check the return string for the specified format

  protected void checkXML(String string) {
    System.err.println("xml:"+string);
    assertTrue(string.indexOf("item")>=0);
  }

  protected void checkJson(String string) {
    System.err.println("json:"+string);
    assertTrue(string.indexOf("{")>=0);
  }

  protected void checkCsv(String string) {
    System.err.println("csv:"+string);
    assertTrue((string.toLowerCase().indexOf("timestamp")>=0) || 
        (string.toLowerCase().indexOf("job")>=0));
  }
}
