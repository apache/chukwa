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

/**
 * Log4JMetricsContext is a plugin for reporting Hadoop Metrics through
 * syslog protocol.  Usage:
 * 
 * Copy hadoop-metrics.properties file from CHUKWA_HOME/conf to HADOOP_HOME/conf.
 * Copy chukwa-hadoop-*-client.jar and json.jar to HADOOP_HOME/lib
 * 
 */
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.metrics.ContextFactory;
import org.apache.hadoop.metrics.MetricsException;
import org.apache.hadoop.metrics.spi.AbstractMetricsContext;
import org.apache.hadoop.metrics.spi.OutputRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.json.simple.JSONObject;
import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;
import java.util.List;

public class Log4JMetricsContext extends AbstractMetricsContext {
  Logger log = Logger.getLogger(Log4JMetricsContext.class);
  Logger out = null; 
  static final Object lock = new Object();

  /* Configuration attribute names */
  protected static final String PERIOD_PROPERTY = "period";
  protected static final String HOST_PROPERTY = "host";
  protected static final String PORT_PROPERTY = "port";  

  protected int period = 0;
  protected String host = "localhost";
  protected int port = 9095;
  
  /** Creates a new instance of MetricsContext */
  public Log4JMetricsContext() {
  }

  public void init(String contextName, ContextFactory factory) {
    super.init(contextName, factory);
   
    String periodStr = getAttribute(PERIOD_PROPERTY);
    if (periodStr != null) {
      int period = 0;
      try {
        period = Integer.parseInt(periodStr);
      } catch (NumberFormatException nfe) {
      }
      if (period <= 0) {
        throw new MetricsException("Invalid period: " + periodStr);
      }
      setPeriod(period);
      this.period = period;
      log.info("Log4JMetricsContext." + contextName + ".period=" + period);
    }
    String host = getAttribute(HOST_PROPERTY);
    if (host != null) {
      this.host = host;
    }
    String port = getAttribute(PORT_PROPERTY);
    if (port != null) {
      this.port = Integer.parseInt(port);
    }
  }

  @Override
  protected void emitRecord(String contextName, String recordName,
      OutputRecord outRec) throws IOException {
    synchronized (lock) {
      if (out == null) {
        PatternLayout layout = new PatternLayout("%d{ISO8601} %p %c: %m%n");
          
        org.apache.log4j.net.SocketAppender appender = new org.apache.log4j.net.SocketAppender(host, port);
          
        appender.setName("chukwa.metrics." + contextName);
        appender.setLayout(layout);
          
        Logger logger = Logger.getLogger("chukwa.metrics." + contextName);
        logger.setAdditivity(false);
        logger.addAppender(appender);
        appender.activateOptions();
        out = logger;
      }
      JSONObject json = new JSONObject();
      try {
        json.put("contextName", contextName);
        json.put("recordName", recordName);
        json.put("chukwa_timestamp", System.currentTimeMillis());
        json.put("period", period);
        for (String tagName : outRec.getTagNames()) {
          json.put(tagName, outRec.getTag(tagName));
        }
        for (String metricName : outRec.getMetricNames()) {
          json.put(metricName, outRec.getMetric(metricName));
        }
      } catch (Exception e) {
        log.warn("exception in Log4jMetricsContext:" , e);
      }
      out.info(json.toString());
    }
  }

  @Override
  public synchronized Map<String, Collection<OutputRecord>> getAllRecords() {
    Map<String, Collection<OutputRecord>> out = new TreeMap<String, Collection<OutputRecord>>();
/*    for (String recordName : bufferedData.keySet()) {
      RecordMap recordMap = bufferedData.get(recordName);
      synchronized (recordMap) {
        List<OutputRecord> records = new ArrayList<OutputRecord>();
        Set<Entry<TagMap, MetricMap>> entrySet = recordMap.entrySet();
        for (Entry<TagMap, MetricMap> entry : entrySet) {
          OutputRecord outRec = new OutputRecord(entry.getKey(), entry.getValue());
          records.add(outRec);
        }
        out.put(recordName, records);
      }
    }*/
    return out;
  }
}
