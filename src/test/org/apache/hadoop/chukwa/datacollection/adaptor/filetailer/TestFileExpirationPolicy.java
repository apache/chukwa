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
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.conf.Configuration;

public class TestFileExpirationPolicy extends TestCase {

  public void testExpiration() {
    ChukwaAgent agent = null;

    try {
      Configuration conf = new ChukwaConfiguration();
      conf.set("chukwaAgent.control.port", "0");
      agent = new ChukwaAgent(conf);

      FileTailingAdaptor.GRACEFUL_PERIOD = 30 * 1000;

      String adaptorId = agent
          .processAddCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped MyType 0 /myWrongPath"
              + System.currentTimeMillis() + " 0");

      assertTrue(adaptorId != null);

      assertNotNull(agent.getAdaptor(adaptorId));

      Thread.sleep(FileTailingAdaptor.GRACEFUL_PERIOD + 10000);
      assertNull(agent.getAdaptor(adaptorId));

    } catch (Exception e) {
      Assert.fail("Exception in TestFileExpirationPolicy");
    } finally {
      if (agent != null) {
        agent.shutdown();
        try {
          Thread.sleep(2000);
        } catch (Exception ex) {
        }
      }
    }

  }

  public void testExpirationOnFileThatHasBennDeleted() {
    ChukwaAgent agent = null;
    File testFile = null;
    try {

      File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
      if (!tempDir.exists()) {
        tempDir.mkdirs();
      }
      String logFile = tempDir.getPath() + "/chukwatestExpiration.txt";
      testFile = makeTestFile(logFile, 8000);

      Configuration conf = new ChukwaConfiguration();
      conf.set("chukwaAgent.control.port", "0");
      agent = new ChukwaAgent(conf);
      // Remove any adaptor left over from previous run

      ChukwaAgentController cli = new ChukwaAgentController("localhost", agent.getControllerPort());
      cli.removeAll();
      // sleep for some time to make sure we don't get chunk from existing
      // streams
      Thread.sleep(5000);

      assertTrue(testFile.canRead() == true);

      FileTailingAdaptor.GRACEFUL_PERIOD = 30 * 1000;
      String adaptorId = agent
          .processAddCommand("add org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped MyType 0 "
              + logFile + " 0");

      assertTrue(adaptorId != null);

      assertNotNull(agent.getAdaptor(adaptorId));

      Thread.sleep(10000);
      testFile.delete();

      Thread.sleep(FileTailingAdaptor.GRACEFUL_PERIOD + 10000);
      assertNull(agent.getAdaptor(adaptorId));
      agent.shutdown();
      Thread.sleep(2000);
    } catch (Exception e) {
      Assert.fail("Exception in TestFileExpirationPolicy");
    } finally {
      if (agent != null) {
        agent.shutdown();
        try {
          Thread.sleep(2000);
        } catch (Exception ex) {
        }
      }
    }
  }

  private File makeTestFile(String name, int size) throws IOException {
    File tmpOutput = new File(name);
    FileOutputStream fos = new FileOutputStream(tmpOutput);

    PrintWriter pw = new PrintWriter(fos);
    for (int i = 0; i < size; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.flush();
    pw.close();
    return tmpOutput;
  }
}
