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

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.sender.RetryListOfCollectors;
import org.apache.hadoop.chukwa.datacollection.writer.NullWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineStageWriter;
import org.apache.hadoop.chukwa.util.ConstRateAdaptor;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

public class TestBackpressure extends TestCase {

  int PORTNO = 9991;
  
  /**
   * NOTE THAT WRITE-RATE * POST SIZE MUST BE GREATER THAN TEST DURATION
   * 
   * Default max post size is 2 MB; need to process that several times during test.
   */
  int TEST_DURATION_SECS = 40;
  int WRITE_RATE_KB = 200; //kb/sec
  
  
  int SEND_RATE = 2500* 1000; //bytes/sec
  int MIN_ACCEPTABLE_PERCENT = 60;
  
  public void testBackpressure() throws Exception {
    Configuration conf = new Configuration();
    conf.set("chukwaCollector.writerClass", NullWriter.class
        .getCanonicalName());
    conf.set(NullWriter.RATE_OPT_NAME, ""+WRITE_RATE_KB);//kb/sec
    conf.setInt(HttpConnector.MIN_POST_INTERVAL_OPT, 100);
    conf.setInt("constAdaptor.sleepVariance", 1);
    conf.setInt("constAdaptor.minSleep", 50);
    
    conf.setInt("chukwaAgent.control.port", 0);
    ChukwaAgent agent = new ChukwaAgent(conf);
    RetryListOfCollectors clist = new RetryListOfCollectors(conf);
    clist.add("http://localhost:"+PORTNO+"/chukwa");
    HttpConnector conn = new HttpConnector(agent);
    conn.setCollectors(clist);
    conn.start();
    Server server = new Server(PORTNO);
    Context root = new Context(server, "/", Context.SESSIONS);

    root.addServlet(new ServletHolder(new ServletCollector(conf)), "/*");
    server.start();
    server.setStopAtShutdown(false);
    Thread.sleep(1000);
    agent.processAddCommand("add adaptor_constSend = " + ConstRateAdaptor.class.getCanonicalName() + 
        " testData "+ SEND_RATE + " 0");
    assertNotNull(agent.getAdaptor("adaptor_constSend"));
    Thread.sleep(TEST_DURATION_SECS * 1000);

    String[] stat = agent.getAdaptorList().get("adaptor_constSend").split(" ");
    long kbytesPerSec = Long.valueOf(stat[stat.length -1]) / TEST_DURATION_SECS / 1000;
    System.out.println("data rate was " + kbytesPerSec + " kb /second");
    assertTrue(kbytesPerSec < WRITE_RATE_KB); //write rate should throttle sends
    assertTrue(kbytesPerSec > MIN_ACCEPTABLE_PERCENT* WRITE_RATE_KB / 100);//an assumption, but should hold true
    agent.shutdown();
  }
}
