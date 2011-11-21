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
package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.ChunkCatcherConnector;
import org.apache.hadoop.conf.Configuration;
import static org.apache.hadoop.chukwa.util.TempFileUtil.*;

public class TestCharFileTailingAdaptorUTF8 extends TestCase {
  ChunkCatcherConnector chunks;
  File baseDir;
  public TestCharFileTailingAdaptorUTF8() {

    baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    chunks = new ChunkCatcherConnector();
    chunks.start();
  }

  public void testCrSepAdaptor() throws IOException, InterruptedException,
      ChukwaAgent.AlreadyRunningException {
    
    Configuration conf = new Configuration();
    conf.set("chukwaAgent.control.port", "0");
    ChukwaAgent agent = new ChukwaAgent(conf);
    File testFile = makeTestFile("chukwaTest", 80,baseDir);
    String adaptorId = agent
        .processAddCommand("add adaptor_test = org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8"
            + " lines " + testFile + " 0");
    assertTrue(adaptorId.equals("adaptor_test"));
    System.out.println("getting a chunk...");
    Chunk c = chunks.waitForAChunk();
    assertTrue(c.getSeqID() == testFile.length());

    assertTrue(c.getRecordOffsets().length == 80);
    int recStart = 0;
    for (int rec = 0; rec < c.getRecordOffsets().length; ++rec) {
      String record = new String(c.getData(), recStart,
          c.getRecordOffsets()[rec] - recStart + 1);
      assertTrue(record.equals(rec + " abcdefghijklmnopqrstuvwxyz\n"));
      recStart = c.getRecordOffsets()[rec] + 1;
    }
    assertTrue(c.getDataType().equals("lines"));
    agent.stopAdaptor(adaptorId, false);
    agent.shutdown();
    Thread.sleep(2000);
  }

  

}
