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
package org.apache.hadoop.chukwa.tools.backfilling;

import java.io.File;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.adaptor.*;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorFactory;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class BackfillingLoader {
  static Logger log = Logger.getLogger(BackfillingLoader.class);
  
  protected Configuration conf = null;
  protected ChunkQueue queue = null;
  protected Connector connector = null;
  
  private String cluster =  null;
  private String machine =  null;
  private String adaptorName =  null;
  private String recordType =  null;
  private String logFile =  null;
  
  public BackfillingLoader(Configuration conf, String cluster, String machine, 
      String adaptorName, String recordType, String logFile) {
    
    this.conf = conf;
    this.cluster = cluster.trim();
    this.machine = machine.trim();
    this.adaptorName = adaptorName;
    this.recordType = recordType;
    this.logFile = logFile;
    
    log.info("cluster >>>" + cluster) ;
    log.info("machine >>>" + machine) ;
    log.info("adaptorName >>>" + adaptorName) ;
    log.info("recordType >>>" + recordType) ;
    log.info("logFile >>>" + logFile) ;
    
    // Set the right cluster and machine information
    DataFactory.getInstance().addDefaultTag("cluster=\"" + this.cluster + "\"");
    ChunkImpl.setHostAddress(this.machine);
    
    queue = DataFactory.getInstance().getEventQueue();
    connector = new QueueToWriterConnector(conf,true);
  }
  
  public void process() throws AdaptorException {
    File file = new File(logFile);
    connector.start();
    Adaptor adaptor = AdaptorFactory.createAdaptor(adaptorName);
    adaptor.parseArgs(recordType, "0 " +file.getAbsolutePath(),AdaptorManager.NULL);
    adaptor.start("", recordType,  0l,queue);
    adaptor.shutdown(AdaptorShutdownPolicy.WAIT_TILL_FINISHED);
    connector.shutdown();
    file.renameTo(new File(logFile + ".sav"));
  }
  
  public static void usage() {
    System.out.println("java org.apache.hadoop.chukwa.tools.backfilling.BackfillingLoader <cluster> <machine> <adaptorName> <recordType> <logFile>");
    System.exit(-1);
  }
  
  /**
   * @param args
   * @throws Exception 
   */
  public static void main(String[] args) throws Exception {

    if (args.length != 5) {
      usage();
    }
    

    String cluster = args[0];
    String machine = args[1];
    String adaptorName = args[2];
    String recordType = args[3];
    String logFile = args[4];

    BackfillingLoader loader = new BackfillingLoader(new ChukwaConfiguration(),cluster,machine,adaptorName,recordType,logFile);
    loader.process();
  }

}
