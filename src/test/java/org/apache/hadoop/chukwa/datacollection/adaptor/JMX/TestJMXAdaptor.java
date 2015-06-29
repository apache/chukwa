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

package org.apache.hadoop.chukwa.datacollection.adaptor.JMX;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.Configuration;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

public class TestJMXAdaptor extends TestCase{
	MBeanServer mbs;
	ChukwaAgent agent;
	File baseDir, checkpointDir;
	@Override
	protected void setUp() throws Exception {
		super.setUp();
		mbs = JMXAgent.getMBeanServerInstance();
		baseDir = new File(System.getProperty("test.build.data", "/tmp")).getCanonicalFile();
	    checkpointDir = new File(baseDir, "addAdaptorTestCheckpoints");		
	    createEmptyDir(checkpointDir);
	    
	    Configuration conf = new Configuration();	    
	    conf.set("chukwaAgent.checkpoint.dir", checkpointDir.getCanonicalPath());
	    conf.set("chukwaAgent.checkpoint.name", "checkpoint_");
	    conf.setInt("chukwaAgent.control.port", 9093);
	    conf.setInt("chukwaAgent.http.port", 9090);
	    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);	    
	    
	    agent = ChukwaAgent.getAgent(conf);
	    agent.start();
	}
	
	public void testJMXAdaptor() {
		MXBeanImpl mxbean = null;
		try{
			mxbean = new MXBeanImpl();
			ObjectName name = new ObjectName("chukwa:type=test");
			mbs.registerMBean(mxbean, name);
		} catch(Exception e){
			e.printStackTrace();
			fail("Failed to instantiate and register test mbean");
		}
		
		Map<Integer, String> m = new HashMap<Integer, String>() {
			private static final long serialVersionUID = 1L;
			{
				put(1, "a");
				put(2, "b");
				put(3, "c");
			}
		};
		Queue<String> queue = new ArrayBlockingQueue<String>(10);
		
		queue.add("Message1");
		queue.add("Message2");
		queue.add("Message3");		
		
		String[] sarray = new String[] {"Screw", "you", "guys", "I'm", "going", "home"};
		
		mxbean.setQueue(queue);
		mxbean.setInt(20);
		mxbean.setMap(m);
		mxbean.setString("TestString");
		mxbean.setStringArray(sarray);
				
	    assertEquals(0, agent.adaptorCount());
	    System.out.println("adding jmx adaptor");
	    String id = agent.processAddCommand("add JMXAdaptor DebugProcessor localhost 10100 10 chukwa:* 0");
	    assertEquals(1, agent.adaptorCount());
	    
	    //A thread that can block on ChunkQueue and can be interrupted
	    class Collector implements Runnable {
	    	String fail = null;
    		public String getFailMessage(){
    			return fail;
    		}
    		public void run(){
    			try {
		        	ChunkQueue eventQueue = DataFactory.getInstance().getEventQueue();    	    
		    	    List<Chunk> evts = new ArrayList<Chunk>();
					eventQueue.collect(evts, 1);
					
					// Expected - {"CompositeType":"3","String":"TestString","StringArray":6,"Map":"3","Int":20}
					
					for (Chunk e : evts) {
			        	String data = new String(e.getData());
			        	JSONObject obj = (JSONObject) JSONValue.parse(data);				        	
			        	assertEquals(obj.get("CompositeType"), "3");
			        	assertEquals(obj.get("String"), "TestString");	        	
			        	assertEquals(obj.get("StringArray"), "6");
			        	assertEquals(obj.get("Map"), "3");
			        	assertEquals(obj.get("Int").toString(), "20");
			        	System.out.println("Verified all data collected by JMXAdaptor");
			        }
				} catch (InterruptedException e1) {			
					e1.printStackTrace();
					fail = "JMXAdaptor failed to collect all data; it was interrupted";
				} catch (AssertionFailedError e2) {
					e2.printStackTrace();
					fail = "Assert failed while verifying JMX data- "+e2.getMessage();
				} catch (Exception e3) {
					e3.printStackTrace();
					fail = "Exception in collector thread. Check the test output for stack trace";
				}        			
    		}
	    }
	    
        try {
        	Collector worker = new Collector();
        	Thread t = new Thread(worker);      	
        	t.start();
        	t.join(20000);
        	if(t.isAlive()){
        		t.interrupt();
        		fail("JMXAdaptor failed to collect data after 20s. Check agent log and surefire report");
        	}
        	String failMessage = worker.getFailMessage();
        	if(failMessage != null){
        		fail(failMessage);
        	}
        	
        } catch(Exception e){
        	e.printStackTrace();
        	fail("Exception in TestJMXAdaptor");
        }

	    System.out.println("shutting down jmx adaptor");
	    agent.stopAdaptor(id, true);
	    assertEquals(0, agent.adaptorCount());
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
	
	@Override
	protected void tearDown() throws Exception {
		nukeDirContents(checkpointDir);
	    checkpointDir.delete();	   
		super.tearDown();
	}
}
