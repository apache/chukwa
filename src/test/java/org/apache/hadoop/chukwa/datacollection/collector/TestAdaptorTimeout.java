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
import java.io.File;
import org.apache.hadoop.chukwa.datacollection.adaptor.TestDirTailingAdaptor;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorResetThread;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.CommitCheckServlet;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.sender.AsyncAckSender;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.chukwa.util.ConstRateAdaptor;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Server;
import junit.framework.TestCase;



public class TestAdaptorTimeout extends TestCase {
  static final int PORTNO = 9997;
  static final int TEST_DURATION_SECS = 30;
  static int SEND_RATE = 10* 1000; //bytes/sec

  public void testAdaptorTimeout() throws Exception {
    Configuration conf = new Configuration();

    String outputDirectory = TestDelayedAcks.buildConf(conf);
    conf.setInt(AdaptorResetThread.TIMEOUT_OPT, 1000);
    ServletCollector collector = new ServletCollector(conf);
    Server collectorServ = TestDelayedAcks.startCollectorOnPort(conf, PORTNO, collector);
    Thread.sleep(1000);
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    HttpConnector conn = new HttpConnector(agent, "http://localhost:"+PORTNO+"/");
    conn.start();
    String resp = agent.processAddCommand("add constSend = " + ConstRateAdaptor.class.getCanonicalName() + 
        " testData "+ SEND_RATE + " 0");
    assertTrue("adaptor_constSend".equals(resp));
    Thread.sleep(TEST_DURATION_SECS * 1000);
    
    AsyncAckSender sender = (AsyncAckSender)conn.getSender();
    int resets = sender.adaptorReset.getResetCount();
    System.out.println(resets + " resets");
    assertTrue(resets > 0);
    
    agent.shutdown();
    collectorServ.stop();
    conn.shutdown();
    Thread.sleep(5000); //for collector to shut down
    
    long dups = TestFailedCollectorAck.checkDirs(conf, conf.get(SeqFileWriter.OUTPUT_DIR_OPT));
    assertTrue(dups > 0);
    TestDirTailingAdaptor.nukeDirContents(new File(outputDirectory));
  }

}
