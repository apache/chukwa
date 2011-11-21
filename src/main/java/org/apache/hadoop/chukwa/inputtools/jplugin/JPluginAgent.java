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

package org.apache.hadoop.chukwa.inputtools.jplugin;

import java.util.Calendar;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.PidFile;

public class JPluginAgent {
  private static Log log = LogFactory.getLog(JPluginAgent.class);

  private static class MetricsTimerTask extends TimerTask {
    @SuppressWarnings( { "unchecked" })
    private JPlugin plugin;

    @SuppressWarnings("unchecked")
    public MetricsTimerTask(JPlugin plugin) {
      this.plugin = plugin;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      try {
        ChukwaMetricsList metrics = plugin.getMetrics();
        String xml = metrics.toXml();
        log.info(xml);
      } catch (Throwable e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  private static class StatusTimerTask extends TimerTask {
    @SuppressWarnings( { "unchecked" })
    private JPlugin plugin;

    @SuppressWarnings("unchecked")
    public StatusTimerTask(JPlugin plugin) {
      this.plugin = plugin;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      try {
        ChukwaMetricsList metrics = plugin.getStatus();
        String xml = metrics.toXml();
        log.info(xml);
      } catch (Throwable e) {
        log.error(e.getMessage(), e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public static void main(String[] args) {
    if (args.length < 1) {
      System.out
          .println("Usage: java -DPERIOD=nn JavaPluginAgent <class name> [parameters]");
      System.exit(0);
    }

    int period = -1;
    try {
      if (System.getProperty("PERIOD") != null) {
        period = Integer.parseInt(System.getProperty("PERIOD"));
      }
    } catch (NumberFormatException ex) {
      ex.printStackTrace();
      System.out.println("PERIOD should be numeric format of seconds.");
      System.exit(0);
    }

    JPlugin plugin = null;
    try {
      plugin = (JPlugin) Class.forName(args[0]).newInstance();
      plugin.init(args);
    } catch (Throwable e) {
      e.printStackTrace();
      System.exit(-1);
    }

    try {
      DaemonWatcher.createInstance(plugin.getRecordType() + "-data-loader");
    } catch (Exception e) {
      e.printStackTrace();
    }

    Calendar cal = Calendar.getInstance();
    long now = cal.getTime().getTime();
    cal.set(Calendar.SECOND, 3);
    cal.set(Calendar.MILLISECOND, 0);
    cal.add(Calendar.MINUTE, 1);
    long until = cal.getTime().getTime();
    try {
      if (period == -1) {
        new MetricsTimerTask(plugin).run();
      } else {
        Thread.sleep(until - now);
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new MetricsTimerTask(plugin), 0,
            period * 1000);
      }
    } catch (Exception ex) {
    }
  }
}
