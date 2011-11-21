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
import java.io.IOException;
import java.util.*;
import java.util.regex.*;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.chukwa.datacollection.adaptor.TestDirTailingAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.FileTailingAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.TestRawAdaptor;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorResetThread;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.CommitCheckServlet;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.sender.*;
import org.apache.hadoop.chukwa.datacollection.sender.ChukwaHttpSender.CommitListEntry;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.chukwa.extraction.archive.SinkArchiver;
import org.apache.hadoop.chukwa.util.ConstRateAdaptor;
import org.apache.hadoop.chukwa.util.ConstRateValidator.ByteRange;
import org.apache.hadoop.chukwa.util.ConstRateValidator.ValidatorSM;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import junit.framework.TestCase;
import static org.apache.hadoop.chukwa.datacollection.sender.AsyncAckSender.DelayedCommit;
import static org.apache.hadoop.chukwa.util.TempFileUtil.*;

public class TestDelayedAcks extends TestCase {
  
  static final int PORTNO = 9993;
  static int END2END_TEST_SECS = 30;
  static int SEND_RATE = 180* 1000; //bytes/sec
  static int CLIENT_SCANPERIOD = 1000;
  static int SERVER_SCANPERIOD = 1000;
  static int ROTATEPERIOD = 2000;
  
  int ACK_TIMEOUT = 200;
  

//start an adaptor -- chunks should appear in the connector
    //wait for timeout.  More chunks should appear.
  public void testAdaptorTimeout() throws Exception {
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    conf.setInt("chukwaAgent.adaptor.context.switch.time", 500);
    conf.setInt(AdaptorResetThread.TIMEOUT_OPT, ACK_TIMEOUT);

    ChukwaAgent agent = new ChukwaAgent(conf);
    ChunkCatcherConnector chunks = new ChunkCatcherConnector();
    chunks.start();
    assertEquals(0, agent.adaptorCount());
    File testFile = makeTestFile("testDA", 50, new File(System.getProperty("test.build.data", "/tmp")));
    long len = testFile.length();
    System.out.println("wrote data to " + testFile);
    AdaptorResetThread restart = new AdaptorResetThread(conf, agent);
    //start timeout thread
    agent.processAddCommand("add fta = "+ FileTailingAdaptor.class.getCanonicalName()
        + " testdata " + testFile.getCanonicalPath() + " 0" );
    
    
    assertEquals(1, agent.adaptorCount());
    Chunk c1 = chunks.waitForAChunk();
    assertNotNull(c1);
    List<CommitListEntry> pendingAcks = new ArrayList<CommitListEntry>();
    pendingAcks.add(new DelayedCommit(c1.getInitiator(), c1.getSeqID(),
        c1.getData().length, "foo", c1.getSeqID(), agent.getAdaptorName(c1.getInitiator())));
    restart.reportPending(pendingAcks);

    assertEquals(len, c1.getData().length);
    Thread.sleep(ACK_TIMEOUT*2);
    int resetCount = restart.resetTimedOutAdaptors(ACK_TIMEOUT);
    Chunk c2 = chunks.waitForAChunk(1000);
    assertNotNull(c2);
    assertEquals(len, c2.getData().length);
    assertTrue(resetCount > 0);
    agent.shutdown();
    
    testFile.delete();
  }
  
  /*
   * Checks the CommitCheckServlet works correctly with a one-chunk file.
   */
  public void testDelayedAck() throws Exception {
    Configuration conf = new Configuration();

    SeqFileWriter writer = new SeqFileWriter();

    conf.set("writer.hdfs.filesystem", "file:///");
    
    File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    
    String outputDirectory = tempDir.getPath() + "/test_DA" + System.currentTimeMillis();

    String seqWriterOutputDir = outputDirectory +"/seqWriter/seqOutputDir";
    conf.set(SeqFileWriter.OUTPUT_DIR_OPT, seqWriterOutputDir );

    writer.init(conf);
    ArrayList<Chunk> oneChunk = new ArrayList<Chunk>();
    oneChunk.add(new ChunkImpl("dt", "name", 1, new byte[] {'b'}, null));

    ChukwaWriter.CommitStatus cs = writer.add(oneChunk);
    writer.close();
    
    File seqWriterFile = null;
    File directory = new File(seqWriterOutputDir);
    String[] files = directory.list();
    for(String file: files) {
      if ( file.endsWith(".done") ){
        seqWriterFile = new File(directory, file);
        break;
      }
    }
    long lenWritten = seqWriterFile.length();
    System.out.println("wrote " + lenWritten+ " bytes");
    assertTrue(cs instanceof ChukwaWriter.COMMIT_PENDING);
    ChukwaWriter.COMMIT_PENDING pending = (ChukwaWriter.COMMIT_PENDING) cs;
    assertTrue(pending.pendingEntries.size() == 1);
    String res = pending.pendingEntries.get(0);
    System.out.println("result was " + res);
    
    Pattern expectedPat= Pattern.compile(".* ([0-9]+)\n");
    Matcher match = expectedPat.matcher(res);
    assertTrue(match.matches());
    long bytesPart = Long.parseLong(match.group(1));
    assertEquals(bytesPart, lenWritten);
  }
  

  public static Server startCollectorOnPort(Configuration conf, int port, 
      ServletCollector collector) throws Exception {
    Server server = new Server(port);
    
    Context root = new Context(server, "/", Context.SESSIONS);
    root.addServlet(new ServletHolder(collector), "/*");
    root.addServlet(new ServletHolder(new CommitCheckServlet(conf)), "/"+CommitCheckServlet.DEFAULT_PATH);

    server.start();
    server.setStopAtShutdown(false);
    return server;
  }
  

  public static String buildConf(Configuration conf) {
    File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    
    String outputDirectory = tempDir.getPath() + "/test_DA" + System.currentTimeMillis() ;

    conf.setInt("chukwaCollector.rotateInterval", ROTATEPERIOD);
    conf.set("writer.hdfs.filesystem", "file:///");
    String seqWriterOutputDir = outputDirectory +"/chukwa_sink";
    conf.set(SeqFileWriter.OUTPUT_DIR_OPT, seqWriterOutputDir );
    conf.setInt(AsyncAckSender.POLLPERIOD_OPT, CLIENT_SCANPERIOD);
    conf.setInt(CommitCheckServlet.SCANPERIOD_OPT, SERVER_SCANPERIOD);
    conf.setBoolean(HttpConnector.ASYNC_ACKS_OPT, true);
    conf.setInt(HttpConnector.MIN_POST_INTERVAL_OPT, 100);
    conf.setInt(HttpConnector.MAX_SIZE_PER_POST_OPT, 10 * 1000*1000);
    conf.setInt(SeqFileWriter.STAT_PERIOD_OPT, 60*60*24); 
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    //turn off stats reporting thread, so we can use Writer.dataSize
    conf.set(AsyncAckSender.POLLHOSTS_OPT, "afilethatdoesntexist");
      //so that it won't try to read conf/collectors
    conf.setInt("chukwaAgent.control.port", 0);
    return outputDirectory;
  }
  
  public void testEndToEnd() {
    try {
      Configuration conf = new Configuration();

      String outputDirectory = buildConf(conf);
      ServletCollector collector = new ServletCollector(conf);
      Server collectorServ = startCollectorOnPort(conf, PORTNO, collector);
      Thread.sleep(1000);
      
      ChukwaAgent agent = new ChukwaAgent(conf);
      HttpConnector conn = new HttpConnector(agent, "http://localhost:"+PORTNO+"/");
      conn.start();
      String resp = agent.processAddCommand("add constSend = " + ConstRateAdaptor.class.getCanonicalName() + 
          " testData "+ SEND_RATE + " 0");
      assertTrue("adaptor_constSend".equals(resp));
      Thread.sleep(END2END_TEST_SECS * 1000);

      //do the shutdown directly, here, so that acks are still processed.
      assertNotNull(agent.getAdaptor("adaptor_constSend"));
      long bytesOutput = agent.getAdaptor("adaptor_constSend").shutdown(AdaptorShutdownPolicy.GRACEFULLY);
      Thread.sleep(CLIENT_SCANPERIOD + SERVER_SCANPERIOD + ROTATEPERIOD + 3000);
      
      String[] stat = agent.getAdaptorList().get("adaptor_constSend").split(" ");
      long bytesCommitted = Long.valueOf(stat[stat.length -1]);
      
      long bytesPerSec = bytesOutput / (1000 * END2END_TEST_SECS);
      System.out.println("data rate was " + bytesPerSec + " kb /second");
   
      //all data should be committed
      System.out.println(bytesCommitted + " bytes committed");
      System.out.println(bytesOutput + " bytes output");
      System.out.println("difference is " + (bytesOutput - bytesCommitted));
      ChukwaWriter w = collector.getWriter();
      long bytesWritten = ((SeqFileWriter)w).getBytesWritten();
      System.out.println("collector wrote " + bytesWritten);

      assertEquals(bytesCommitted, bytesOutput);
      assertEquals(bytesWritten, bytesCommitted);
      //We need a little imprecision here, since the send rate is a bit bursty,
      //and since some acks got lost after the adaptor was stopped.
      assertTrue(bytesPerSec > 9 * SEND_RATE/ 1000 / 10);
      AsyncAckSender sender = (AsyncAckSender)conn.getSender();
      assertEquals(0, sender.adaptorReset.getResetCount());
      
      agent.shutdown();
      collectorServ.stop();
      conn.shutdown();
      Thread.sleep(5000); //for collector to shut down
      TestDirTailingAdaptor.nukeDirContents(new File(outputDirectory));
      
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }

  }


}
