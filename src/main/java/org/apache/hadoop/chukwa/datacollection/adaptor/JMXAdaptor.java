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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.rmi.ConnectException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import javax.management.Descriptor;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.TabularData;
import javax.management.openmbean.TabularType;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;

/**
 * Query metrics through JMX interface. <br>
 * 1. Enable remote jmx monitoring for the target
 * jvm by specifying -Dcom.sun.management.jmxremote.port=jmx_port<br>
 * 2. Enable authentication with -Dcom.sun.management.jmxremote.authenticate=true <br>
 *    -Dcom.sun.management.jmxremote.password.file=${CHUKWA_CONF_DIR}/jmxremote.password <br>
 *    -Dcom.sun.management.jmxremote.access.file=${CHUKWA_CONF_DIR}/jmxremote.access <br>
 * 3. Optionally specify these jvm options <br>
 * 	  -Djava.net.preferIPv4Stack=true -Dcom.sun.management.jmxremote.ssl=false <br>
 * 4. Connect to the jmx agent using jconsole and find out which domain you want to collect data for
 * 5. Add the jmx adaptor. Ex: To collect metrics from a hadoop datanode that has enabled jmx on 8007, at 60s interval, use command<br>
 *   "add JMXAdaptor DatanodeProcessor localhost 8007 60 Hadoop:*" <br><br>
 * Your JMX adaptor is now good to go and will send out the collected metrics as chunks to the collector.
 */
public class JMXAdaptor extends AbstractAdaptor{

	private static Logger log = Logger.getLogger(JMXAdaptor.class);
	private MBeanServerConnection mbsc = null;
	private String port ="", server="localhost";	
	private JMXServiceURL url;
	private JMXConnector jmxc = null;
	private long period = 10;
	private Timer timer;
	private JMXTimer runner;
	private String pattern = "";
	long sendOffset = 0;
	volatile boolean shutdown = false;
	
	/**
	 * A thread which creates a new connection to JMX and retries every 10s if the connection is not  
	 * successful. It uses the credentials specified in $CHUKWA_CONF_DIR/jmxremote.password.
	 */
	public class JMXConnect implements Runnable{

		@Override
		public void run() {
			String hadoop_conf = System.getenv("CHUKWA_CONF_DIR");
			StringBuffer sb = new StringBuffer(hadoop_conf);
			if(!hadoop_conf.endsWith("/")){
				sb.append(File.separator);
			}
			sb.append("jmxremote.password");
			String jmx_pw_file = sb.toString();
			shutdown = false;
			while(!shutdown){
				try{					
					BufferedReader br = new BufferedReader(new FileReader(jmx_pw_file));
					String[] creds = br.readLine().split(" ");
					Map<String, String[]> env = new HashMap<String, String[]>();			
					env.put(JMXConnector.CREDENTIALS, creds);
					jmxc = JMXConnectorFactory.connect(url, env);
					mbsc = jmxc.getMBeanServerConnection();							
					if(timer == null) {
						timer = new Timer();
						runner = new JMXTimer(dest, JMXAdaptor.this,mbsc);
					}
					timer.scheduleAtFixedRate(runner, 0, period * 1000);
					shutdown = true;
				} catch (IOException e) {
					log.error("IOException in JMXConnect thread prevented connect to JMX on port:"+port+", retrying after 10s");
					log.error(ExceptionUtil.getStackTrace(e));	
					try {
						Thread.sleep(10000);
					} catch (InterruptedException e1) {
						log.error("JMXConnect thread interrupted in sleep, bailing");
						shutdown = true;
					}
				} catch (Exception e) {
					log.error("Something bad happened in JMXConnect thread, bailing");
					log.error(ExceptionUtil.getStackTrace(e));
					timer.cancel();
					timer = null;
					shutdown = true;
				}						
			}
		}
		
	}
	
	/**
	 * A TimerTask which queries the mbean server for all mbeans that match the pattern specified in
	 * the JMXAdaptor arguments, constructs a json object of all data and sends it as a chunk. The 
	 * CompositeType, TabularType and Array open mbean types return the numerical values (sizes). 
	 * This task is scheduled to run at the interval specified in the adaptor arguments. If the 
	 * connection to mbean server is broken, this task cancels the existing timer and tries to 
	 * re-connect to the mbean server.  
	 */
	
	public class JMXTimer extends TimerTask{

		private Logger log = Logger.getLogger(JMXTimer.class);
		private ChunkReceiver receiver = null;
		private JMXAdaptor adaptor = null;
		private MBeanServerConnection mbsc = null;
		//private long sendOffset = 0;
		
		public JMXTimer(ChunkReceiver receiver, JMXAdaptor adaptor, MBeanServerConnection mbsc){
			this.receiver = receiver;
			this.adaptor = adaptor;		
			this.mbsc = mbsc;
		}
		
		@SuppressWarnings("unchecked")
		@Override
		public void run() {
			try{
				ObjectName query = null;
				if(!pattern.equals("")){
					query = new ObjectName(pattern);			
				}
				Set<ObjectName> names = new TreeSet<ObjectName>(mbsc.queryNames(query, null));
				Object val = null;
				JSONObject json = new JSONObject();
									
				for (ObjectName oname: names) {			
					MBeanInfo mbinfo = mbsc.getMBeanInfo(oname);
					MBeanAttributeInfo [] mbinfos = mbinfo.getAttributes();						
					
					for (MBeanAttributeInfo mb: mbinfos) {
						try{
							Descriptor d = mb.getDescriptor();
							val = mbsc.getAttribute(oname, mb.getName());
							if(d.getFieldNames().length > 0){ //this is an open mbean
								OpenType openType = (OpenType)d.getFieldValue("openType");	
								
								if(openType.isArray()){									
									Object[] valarray = (Object[])val;									
									val = Integer.toString(valarray.length);
								}
								else if(openType instanceof CompositeType){
									CompositeData data = (CompositeData)val;
									val = Integer.toString(data.values().size());									
								}
								else if(openType instanceof TabularType){
									TabularData data = (TabularData)val;
									val = Integer.toString(data.size());
								}
								//else it is SimpleType									
							}
							json.put(mb.getName(),val);
						}
						catch(Exception e){
							log.warn("Exception "+ e.getMessage() +" getting attribute - "+mb.getName() + " Descriptor:"+mb.getDescriptor().getFieldNames().length);
						}						
					}
				}
				
				byte[] data = json.toString().getBytes();		
				sendOffset+=data.length;				
				ChunkImpl c = new ChunkImpl(type, "JMX", sendOffset, data, adaptor);
				long rightNow = Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTimeInMillis();
				c.addTag("timeStamp=\""+rightNow+"\"");
				receiver.add(c);
			}
			catch(ConnectException e1){
				log.error("Got connect exception for the existing MBeanServerConnection");
				log.error(ExceptionUtil.getStackTrace(e1));
				log.info("Make sure the target process is running. Retrying connection to JMX on port:"+port);
				timer.cancel();
				timer = null;
				Thread connectThread = new Thread(new JMXConnect());
				connectThread.start();
			}
			catch(Exception e){
				log.error(ExceptionUtil.getStackTrace(e));
			}
			
		}
		
	}
	
	
	@Override
	public String getCurrentStatus() {
		StringBuilder buffer = new StringBuilder();
		buffer.append(type);
		buffer.append(" ");
		buffer.append(server);
		buffer.append(" ");
		buffer.append(port);
		buffer.append(" ");
		buffer.append(period);
		buffer.append(" ");
		buffer.append(pattern);
		return buffer.toString();
	}
	
	@Override
	public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
			throws AdaptorException {
		log.info("Enter Shutdown:" + shutdownPolicy.name()+ " - ObjectId:" + this);
		try {
			if(jmxc != null){
				jmxc.close();
			}
			if(timer != null){
				timer.cancel();
			}
		} catch (IOException e) {
			log.error("JMXAdaptor shutdown failed due to IOException");
			throw new AdaptorException(ExceptionUtil.getStackTrace(e));
		} catch (Exception e) {
			log.error("JMXAdaptor shutdown failed");
			throw new AdaptorException(ExceptionUtil.getStackTrace(e));
		}
		//in case the start thread is still retrying
		shutdown = true;
	    return sendOffset;
		
		
	}

	@Override
	public void start(long offset) throws AdaptorException {
		try {			
			sendOffset = offset;
			Thread connectThread = new Thread(new JMXConnect());
			connectThread.start();			
		} catch(Exception e) {
			log.error("Failed to schedule JMX connect thread");
			throw new AdaptorException(ExceptionUtil.getStackTrace(e));	
		}
		
	}

	@Override
	public String parseArgs(String s) {
		//JMXAdaptor MBeanServer port [interval] DomainNamePattern-Ex:"Hadoop:*"
		String[] tokens = s.split(" ");
		if(tokens.length == 4){
			server = tokens[0];
			port = tokens[1];
			period = Integer.parseInt(tokens[2]);
			pattern = tokens[3];
		}
		else if(tokens.length == 3){
			server = tokens[0];
			port = tokens[1];
			pattern = tokens[2];
		}
		else{
			log.warn("bad syntax in JMXAdaptor args");
			return null;
		}
		String url_string = "service:jmx:rmi:///jndi/rmi://"+server+ ":"+port+"/jmxrmi";
		try{
			url = new JMXServiceURL(url_string);			
			return s;
		}
		catch(Exception e){
			log.error(ExceptionUtil.getStackTrace(e));
		}
		return null;		
	}
	
}

