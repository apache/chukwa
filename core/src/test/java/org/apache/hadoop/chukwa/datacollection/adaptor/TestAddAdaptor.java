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
package org.apache.hadoop.chukwa.datacollection.adaptor;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class TestAddAdaptor extends TestCase {
	ChukwaAgent agent = null;
	File baseDir;
	  
  public void testJmxAdd() throws IOException,
      ChukwaAgent.AlreadyRunningException, InterruptedException {
    Configuration conf = new Configuration();
    baseDir = new File(System.getProperty("test.build.data", "/tmp"))
        .getCanonicalFile();
    File checkpointDir = new File(baseDir, "addAdaptorTestCheckpoints");
    createEmptyDir(checkpointDir);

    conf.set("chukwaAgent.checkpoint.dir", checkpointDir.getCanonicalPath());
    conf.set("chukwaAgent.checkpoint.name", "checkpoint_");
    conf.setInt("chukwaAgent.control.port", 0);
    conf.setInt("chukwaAgent.http.port", 9090);
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);

    agent = ChukwaAgent.getAgent(conf);
    agent.start();

    assertEquals(0, agent.adaptorCount());
    System.out.println("adding jmx adaptor");
    String id = agent
        .processAddCommand("add JMXAdaptor DebugProcessor localhost 0 60 hadoop:* 0");
    assertEquals(1, agent.adaptorCount());

    System.out.println("shutting down jmx adaptor");
    agent.stopAdaptor(id, true);
    assertEquals(0, agent.adaptorCount());

    String rest_url = "http://localhost:9090/rest/v2/adaptor";
    System.out.println("adding jmx adaptor using rest url - " + rest_url);

    String dataType = "DebugProcessor", adaptorClass = "JMXAdaptor", adaptorParams = "localhost 0 60 hadoop:*", offset = "0";
    String adaptor_json = "{\"dataType\":\"" + dataType
        + "\", \"adaptorClass\":\"" + adaptorClass
        + "\", \"adaptorParams\" : \"" + adaptorParams + "\", \"offset\" : \""
        + offset + "\" }";
    System.out.println(adaptor_json);
    Client client = Client.create();
    WebResource resource = client.resource(rest_url);
    ClientResponse response = resource.type("application/json").post(
        ClientResponse.class, adaptor_json);
    if (response.getStatus() != 200 && response.getStatus() != 201) {
      fail("Add adaptor through REST failed : HTTP error code : "
          + response.getStatus());
    }
    assertEquals(1, agent.adaptorCount());
    String result = response.getEntity(String.class);

    try {
      JSONObject json = (JSONObject) JSONValue.parse(result);
      id = (String) json.get("id");
    } catch (Exception e) {
      fail("Failed to parse response from add. Complete response is:\n"
          + result);
    }

    System.out.println("shutting down jmx adaptor with id:" + id
        + " through rest");
    resource = client.resource(rest_url + "/" + id);
    response = resource.delete(ClientResponse.class);
    if (response.getStatus() != 200 && response.getStatus() != 201) {
      fail("Delete adaptor through REST failed : HTTP error code : "
          + response.getStatus());
    }

    assertEquals(0, agent.adaptorCount());

    agent.shutdown();
    Thread.sleep(1500);
    nukeDirContents(checkpointDir);
    checkpointDir.delete();
  }
	
	protected void tearDown(){
		if(agent != null){
			agent.shutdown();
		}
	}
	
	 //returns true if dir exists
	  public static boolean nukeDirContents(File dir) {
	    if(dir.exists()) {
	      if(dir.isDirectory()) {
	        for(File f: dir.listFiles()) {
	          nukeDirContents(f);
	          f.delete();
	        }
	      } else
	        dir.delete();
	      
	      return true;
	    }
	    return false;
	  }
	  
	  public static void createEmptyDir(File dir) {
	    if(!nukeDirContents(dir))
	      dir.mkdir();
	    assertTrue(dir.isDirectory() && dir.listFiles().length == 0);
	  }
}
