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

package org.apache.hadoop.chukwa.inputtools.log4j;

import java.io.File;
import java.net.URL;
import java.util.Date;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.helpers.OptionConverter;

public class TestTaskLogAppender extends TestCase {

  @SuppressWarnings("deprecation")
  public void testTaskLogAppender() {

    String folder = System.getProperty("test.build.classes");
    File logFile = new File(folder + "/userlogs/job_200905220200_13470/attempt_200905220200_13470_r_000000_0/syslog");
    if (logFile.exists()) {
      logFile.delete();
    }
    Assert.assertTrue("Log file should not be there", logFile.exists() == false);
    File tempDir = new File(folder);
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    // load new log4j
    String configuratorClassName = OptionConverter.getSystemProperty(
        LogManager.CONFIGURATOR_CLASS_KEY, null);

    URL url = TestTaskLogAppender.class
        .getResource("/tasklog-log4j.properties");

    System.getProperties().setProperty("CHUKWA_LOG_DIR", folder);

    if (url != null) {
      LogLog
          .debug("Using URL [" + url + "] for automatic log4j configuration.");
      try {
        OptionConverter.selectAndConfigure(url, configuratorClassName,
            LogManager.getLoggerRepository());
      } catch (NoClassDefFoundError e) {
        LogLog.warn("Error during default initialization", e);
      }
    } else {
      Assert.fail("URL should not be null");
    }
    Logger log = Logger.getLogger(TestTaskLogAppender.class);
    try {
      Thread.sleep(2000);
    }catch (Exception e) {
      // do nothing
    }
    log.warn("test 123 " + new Date());
    Assert.assertTrue("Log file should exist", logFile.exists() == true);
    logFile.delete();
  }

}
