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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.io.IOException;
import java.nio.charset.Charset;
import java.security.PrivilegedExceptionAction;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.util.ChukwaUtil;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.RestUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public class OozieAdaptor extends AbstractAdaptor {

  private static Logger log = Logger.getLogger(OozieAdaptor.class);
  private String uri;

  private long sendOffset;
  private Configuration chukwaConfiguration = null;
  private static UserGroupInformation UGI = null;
  private boolean isKerberosEnabled = false;
  private int length = 0;
  private final ScheduledExecutorService scheduler = Executors
      .newScheduledThreadPool(1);
  private static final long initialDelay = 60; // seconds
  private static long periodicity = 60; // seconds
  private ScheduledFuture<?> scheduledCollectorThread;

  @Override
  public String parseArgs(String s) {
    String[] tokens = s.split(" ");
    if (tokens.length == 2) {
      uri = tokens[0];
      try {
        periodicity = Integer.parseInt(tokens[1]);
      } catch (NumberFormatException e) {
        log.warn("OozieAdaptor: incorrect argument for period. Expecting number");
        return null;
      }
    } else {
      log.warn("bad syntax in OozieAdaptor args");
      return null;
    }
    return s;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    sendOffset = offset;
    init(); // initialize the configuration
    log.info("Starting Oozie Adaptor with [ " + sendOffset + " ] offset");
    scheduledCollectorThread = scheduler.scheduleAtFixedRate(
        new OozieMetricsCollector(), initialDelay, periodicity,
        TimeUnit.SECONDS);
    log.info("scheduled");
  }

  @Override
  public String getCurrentStatus() {
    StringBuilder buffer = new StringBuilder();
    buffer.append(type);
    buffer.append(" ");
    buffer.append(uri);
    buffer.append(" ");
    buffer.append(periodicity);
    return buffer.toString();
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    scheduledCollectorThread.cancel(true);
    scheduler.shutdown();
    return sendOffset;
  }

  private class OozieMetricsCollector implements Runnable {
    @Override
    public void run() {
      try {
        if (isKerberosEnabled) {
          if (UGI == null) {
            throw new IllegalStateException("UGI Login context is null");
          }

          UGI.checkTGTAndReloginFromKeytab();
          length = UGI.doAs(new PrivilegedExceptionAction<Integer>() {
            @Override
            public Integer run() throws Exception {
              return processMetrics();
            }
          });

        } else {
          length = processMetrics();
        }

        if (length <= 0) {
          log.warn("Oozie is either not responding or sending zero payload");
        } else {
          log.info("Processing a oozie instrumentation payload of [" + length
              + "] bytes");
        }
      } catch (Exception e) {
        log.error(ExceptionUtil.getStackTrace(e));
        log.error("Exception occured while getting oozie metrics " + e);
      }
    }
  }

  private void init() {
    if (getChukwaConfiguration() == null) {
      setChukwaConfiguration(ChukwaUtil.readConfiguration());
    }
    String authType = getChukwaConfiguration().get(
        "chukwaAgent.hadoop.authentication.type");
    if (authType != null && authType.equalsIgnoreCase("kerberos")) {
      login(); // get the UGI context
      isKerberosEnabled = true;
    }
  }

  private void login() {
    try {
      String principalConfig = getChukwaConfiguration().get(
          "chukwaAgent.hadoop.authentication.kerberos.principal",
          System.getProperty("user.name"));
      String hostname = null;
      String principalName = SecurityUtil.getServerPrincipal(principalConfig,
          hostname);
      UGI = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          principalName,
          getChukwaConfiguration().get(
              "chukwaAgent.hadoop.authentication.kerberos.keytab"));
    } catch (IOException e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }

  private int processMetrics() {
    return addChunkToReceiver(getOozieMetrics().getBytes(Charset.forName("UTF-8")));
  }

  private String getOozieMetrics() {
    return RestUtil.getResponseAsString(uri);
  }

  public int addChunkToReceiver(byte[] data) {
    try {
      sendOffset += data.length;
      ChunkImpl c = new ChunkImpl(type, "REST", sendOffset, data, this);
      long rightNow = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
          .getTimeInMillis();
      c.addTag("timeStamp=\"" + rightNow + "\"");
      dest.add(c);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return data.length;
  }

  public Configuration getChukwaConfiguration() {
    return chukwaConfiguration;
  }

  public void setChukwaConfiguration(Configuration chukwaConfiguration) {
    this.chukwaConfiguration = chukwaConfiguration;
  }
}
