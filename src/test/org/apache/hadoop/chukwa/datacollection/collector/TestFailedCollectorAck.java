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
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.adaptor.TestDirTailingAdaptor;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.CommitCheckServlet;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.sender.RetryListOfCollectors;
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
import junit.framework.TestCase;

public class TestFailedCollectorAck extends TestCase {
  
  static final int PORTNO = 9993;

  public void testFailureRecovery() {
    try {
    Configuration conf = new Configuration();

    String outputDirectory = TestDelayedAcks.buildConf(conf);
    SeqFileWriter.ENABLE_ROTATION_ON_CLOSE = false;
    File sinkA = new File(outputDirectory, "chukwa_sink_A");
    sinkA.mkdir();
    File sinkB = new File(outputDirectory, "chukwa_sink_B");
    sinkB.mkdir();
    conf.set(CommitCheckServlet.SCANPATHS_OPT, sinkA.getCanonicalPath()
        + "," + sinkB.getCanonicalPath());
    conf.set(SeqFileWriter.OUTPUT_DIR_OPT, sinkA.getCanonicalPath() );
    ServletCollector collector1 = new ServletCollector(new Configuration(conf));
    conf.set(SeqFileWriter.OUTPUT_DIR_OPT,sinkB.getCanonicalPath() );
    ServletCollector collector2 = new ServletCollector(conf);
    Server collector1_s = TestDelayedAcks.startCollectorOnPort(conf, PORTNO+1, collector1);
    Server collector2_s = TestDelayedAcks.startCollectorOnPort(conf, PORTNO+2, collector2);
    Thread.sleep(2000); //for collectors to start
    
    ChukwaAgent agent = new ChukwaAgent(conf);
    HttpConnector conn = new HttpConnector(agent);
    RetryListOfCollectors clist = new RetryListOfCollectors(conf);
    clist.add("http://localhost:"+(PORTNO+1)+"/");
    clist.add("http://localhost:"+(PORTNO+2)+"/");
    conn.setCollectors(clist);
    conn.start();
    //FIXME: somehow need to clue in commit checker which paths to check.
    //       Somehow need 

    String resp = agent.processAddCommand("add adaptor_constSend = " + ConstRateAdaptor.class.getCanonicalName() + 
        " testData "+ TestDelayedAcks.SEND_RATE + " 12345 0");
    assertTrue("adaptor_constSend".equals(resp));
    Thread.sleep(10 * 1000);
    collector1_s.stop();
    Thread.sleep(10 * 1000);
    SeqFileWriter.ENABLE_ROTATION_ON_CLOSE = true;

    String[] stat = agent.getAdaptorList().get("adaptor_constSend").split(" ");
    long bytesCommitted = Long.valueOf(stat[stat.length -1]);
    assertTrue(bytesCommitted > 0);
    agent.shutdown();
    conn.shutdown();
    Thread.sleep(2000); //for collectors to shut down
    collector2_s.stop();
    Thread.sleep(2000); //for collectors to shut down
    
    checkDirs(conf, conf.get(CommitCheckServlet.SCANPATHS_OPT));
    
    TestDirTailingAdaptor.nukeDirContents(new File(outputDirectory));
    (new File(outputDirectory)).delete();
    } catch(Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
  }
  
  //returns number of dup chunks
  public static long checkDirs(Configuration conf, String paths) throws IOException {
    
    ArrayList<Path> toScan = new ArrayList<Path>();
    ArrayList<ByteRange> bytes = new ArrayList<ByteRange>();
    FileSystem localfs = FileSystem.getLocal(conf);

    String[] paths_s = paths.split(",");
    for(String s: paths_s)
      if(s.length() > 1)
        toScan.add(new Path(s));
    
    for(Path p: toScan) {
      
      FileStatus[] dataSinkFiles = localfs.listStatus(p, SinkArchiver.DATA_SINK_FILTER);
   
      for(FileStatus fstatus: dataSinkFiles) {
        if(!fstatus.getPath().getName().endsWith(".done"))
          continue;
        
        SequenceFile.Reader reader = new SequenceFile.Reader(localfs, fstatus.getPath(), conf);

        ChukwaArchiveKey key = new ChukwaArchiveKey();
        ChunkImpl chunk = ChunkImpl.getBlankChunk();

        while (reader.next(key, chunk)) {
         bytes.add(new ByteRange(chunk));
        }
        reader.close();
      }
    }

    assertNotNull(bytes);
    Collections.sort(bytes);
    
    ValidatorSM sm = new ValidatorSM();
    for(ByteRange b: bytes) {
      String s = sm.advanceSM(b);
      if(s != null)
        System.out.println(s);
    }
    assertEquals(0, sm.missingBytes);
    return sm.dupBytes;
  }

}
