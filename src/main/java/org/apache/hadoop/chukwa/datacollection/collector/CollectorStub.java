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

package org.apache.hadoop.chukwa.datacollection.collector;


import org.mortbay.jetty.*;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.*;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.*;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.writer.*;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import edu.berkeley.confspell.Checker;
import edu.berkeley.confspell.HSlurper;
import edu.berkeley.confspell.OptDictionary;
import javax.servlet.http.HttpServlet;
import java.io.File;
import java.util.*;

public class CollectorStub {

  static int THREADS = 120;
  public static Server jettyServer = null;

  public static void main(String[] args) {

    DaemonWatcher.createInstance("Collector");
    try {
      if (args.length > 0 && (args[0].equalsIgnoreCase("help")|| args[0].equalsIgnoreCase("-help"))) {
        System.out.println("usage: Normally you should just invoke CollectorStub without arguments.");
        System.out.println("A number of options can be specified here for debugging or special uses.  e.g.: ");
        System.out.println("Options include:\n\tportno=<#> \n\t" + "writer=pretend | <classname>"
                    + "\n\tservlet=<classname>@path");
        System.out.println("Command line options will override normal configuration.");
        System.exit(0);
      }

      ChukwaConfiguration conf = new ChukwaConfiguration();
      
      try {
        Configuration collectorConf = new Configuration(false);
        collectorConf.addResource(new Path(conf.getChukwaConf() + "/chukwa-common.xml"));
        collectorConf.addResource(new Path(conf.getChukwaConf() + "/chukwa-collector-conf.xml"));
        Checker.checkConf(new OptDictionary(new File(new File(conf.getChukwaHome(), "share/chukwa/lib"), "collector.dict")),
            HSlurper.fromHConf(collectorConf));
      } catch(Exception e) {e.printStackTrace();}
      
      int portNum = conf.getInt("chukwaCollector.http.port", 9999);
      THREADS = conf.getInt("chukwaCollector.http.threads", THREADS);

      // pick a writer.
      ChukwaWriter w = null;
      Map<String, HttpServlet> servletsToAdd = new TreeMap<String, HttpServlet>();
      ServletCollector servletCollector = new ServletCollector(conf);
      for(String arg: args) {
        if(arg.startsWith("writer=")) {       //custom writer class
          String writerCmd = arg.substring("writer=".length());
          if (writerCmd.equals("pretend") || writerCmd.equals("pretend-quietly")) {
            boolean verbose = !writerCmd.equals("pretend-quietly");
            w = new ConsoleWriter(verbose);
            w.init(conf);
            servletCollector.setWriter(w);
          } else 
            conf.set("chukwaCollector.writerClass", writerCmd);
        } else if(arg.startsWith("servlet=")) {     //adding custom servlet
           String servletCmd = arg.substring("servlet=".length()); 
           String[] halves = servletCmd.split("@");
           try {
             Class<?> servletClass = Class.forName(halves[0]);
             HttpServlet srvlet = (HttpServlet) servletClass.newInstance();
             if(!halves[1].startsWith("/"))
               halves[1] = "/" + halves[1];
             servletsToAdd.put(halves[1], srvlet);
           } catch(Exception e) {
             e.printStackTrace();
           }
        } else if(arg.startsWith("portno=")) {
          portNum = Integer.parseInt(arg.substring("portno=".length()));
        } else { //unknown arg
          System.out.println("WARNING: unknown command line arg " + arg);
          System.out.println("Invoke collector with command line arg 'help' for usage");
        }
      }

      // Set up jetty connector
      SelectChannelConnector jettyConnector = new SelectChannelConnector();
      jettyConnector.setLowResourcesConnections(THREADS - 10);
      jettyConnector.setLowResourceMaxIdleTime(1500);
      jettyConnector.setPort(portNum);
      
      // Set up jetty server proper, using connector
      jettyServer = new Server(portNum);
      jettyServer.setConnectors(new Connector[] { jettyConnector });
      org.mortbay.thread.BoundedThreadPool pool = new org.mortbay.thread.BoundedThreadPool();
      pool.setMaxThreads(THREADS);
      jettyServer.setThreadPool(pool);
      
      // Add the collector servlet to server
      Context root = new Context(jettyServer, "/", Context.SESSIONS);
      root.addServlet(new ServletHolder(servletCollector), "/*");
      
      if(conf.getBoolean(HttpConnector.ASYNC_ACKS_OPT, false))
        root.addServlet(new ServletHolder(new CommitCheckServlet(conf)), "/"+CommitCheckServlet.DEFAULT_PATH);

      if(conf.getBoolean(LogDisplayServlet.ENABLED_OPT, false))
        root.addServlet(new ServletHolder(new LogDisplayServlet(conf)), "/"+LogDisplayServlet.DEFAULT_PATH);

      
      root.setAllowNullPathInfo(false);

      // Add in any user-specified servlets
      for(Map.Entry<String, HttpServlet> e: servletsToAdd.entrySet()) {
        root.addServlet(new ServletHolder(e.getValue()), e.getKey());
      }
      
      // And finally, fire up the server
      jettyServer.start();
      jettyServer.setStopAtShutdown(true);

      System.out.println("started Chukwa http collector on port " + portNum);
      System.out.close();
      System.err.close();
    } catch (Exception e) {
      e.printStackTrace();
      DaemonWatcher.bailout(-1);
    }

  }

}
