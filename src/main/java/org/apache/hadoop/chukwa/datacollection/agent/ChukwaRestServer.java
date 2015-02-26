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

package org.apache.hadoop.chukwa.datacollection.agent;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.mortbay.jetty.AbstractConnector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.security.SslSocketConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.mortbay.thread.QueuedThreadPool;

import com.sun.jersey.spi.container.servlet.ServletContainer;
import static org.apache.hadoop.chukwa.datacollection.agent.ChukwaConstants.*;

public class ChukwaRestServer {
  private Configuration conf;
  private Server jettyServer;  
  private final Logger log = Logger.getLogger(ChukwaRestServer.class);  
  private final String AGENT_HTTP_PORT = "chukwaAgent.http.port";
  private final String AGENT_REST_CONTROLLER_PACKAGES = "chukwaAgent.http.rest.controller.packages";
  private final int HTTP_SERVER_THREADS = 120;
  
  private static ChukwaRestServer instance = null;
  
  public static void startInstance(Configuration conf) throws Exception{
    if(instance == null){
      synchronized(ChukwaRestServer.class) {
        if(instance == null){
          instance = new ChukwaRestServer(conf);
          instance.start();
        }        
      }
    }
  }
  
  public static void stopInstance() throws Exception {
    if(instance != null) {
      synchronized(ChukwaRestServer.class) {
        if(instance != null){
          instance.stop();
          instance = null;
        }
      }
    }
    
  }
  
  private ChukwaRestServer(Configuration conf){
    this.conf = conf;
  }
  
  private void start() throws Exception{
    int portNum = conf.getInt(AGENT_HTTP_PORT, 9090);
    String jaxRsAddlPackages = conf.get(AGENT_REST_CONTROLLER_PACKAGES);
    StringBuilder jaxRsPackages = new StringBuilder(
            "org.apache.hadoop.chukwa.datacollection.agent.rest");
  
    // Allow the ability to add additional servlets to the server
    if (jaxRsAddlPackages != null)
      jaxRsPackages.append(';').append(jaxRsAddlPackages);
  
    // Set up jetty connector
    AbstractConnector jettyConnector;
    if("true".equals(conf.get(SSL_ENABLE))){
      SslSocketConnector sslConnector = new SslSocketConnector();
      sslConnector.setKeystore(conf.get(KEYSTORE_STORE));
      sslConnector.setPassword(conf.get(KEYSTORE_PASSWORD));
      sslConnector.setKeyPassword(conf.get(KEYSTORE_KEY_PASSWORD));
      sslConnector.setKeystoreType(conf.get(KEYSTORE_TYPE, DEFAULT_STORE_TYPE));
      String trustStore = conf.get(TRUSTSTORE_STORE);
      if(trustStore != null){
        sslConnector.setTruststore(trustStore);
        sslConnector.setTrustPassword(conf.get(TRUST_PASSWORD));
        sslConnector.setTruststoreType(conf.get(TRUSTSTORE_TYPE, DEFAULT_STORE_TYPE));
        sslConnector.setNeedClientAuth(false);
      }
      jettyConnector = sslConnector;
    } else {
      jettyConnector = new SelectChannelConnector();
    }
    //jettyConnector.setLowResourcesConnections(HTTP_SERVER_THREADS - 10);
    jettyConnector.setLowResourceMaxIdleTime(1500);
    jettyConnector.setPort(portNum);
    jettyConnector.setReuseAddress(true);
    // Set up jetty server, using connector
    jettyServer = new Server(portNum);
    jettyServer.setConnectors(new org.mortbay.jetty.Connector[] { jettyConnector });
    QueuedThreadPool pool = new QueuedThreadPool();
    pool.setMaxThreads(HTTP_SERVER_THREADS);
    jettyServer.setThreadPool(pool);
  
    // Create the controller servlets
    ServletHolder servletHolder = new ServletHolder(ServletContainer.class);
    servletHolder.setInitParameter("com.sun.jersey.config.property.resourceConfigClass",
            "com.sun.jersey.api.core.PackagesResourceConfig");
    servletHolder.setInitParameter("com.sun.jersey.config.property.packages",
            jaxRsPackages.toString());
  
    // Create the server context and add the servlet
    Context root = new Context(jettyServer, "/rest/v2", Context.SESSIONS);
    root.setAttribute("ChukwaAgent", ChukwaAgent.getAgent());
    root.addServlet(servletHolder, "/*");
    root.setAllowNullPathInfo(false);
  
    // And finally, fire up the server
    jettyServer.start();
    jettyServer.setStopAtShutdown(true);
  
    log.info("started Chukwa http agent interface on port " + portNum);
  }
  
  private void stop() throws Exception{
    jettyServer.stop();
    log.info("Successfully stopped Chukwa http agent interface");
  }
}
