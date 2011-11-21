/**
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

import org.json.simple.JSONObject;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.metrics2.Metric;
import org.apache.hadoop.metrics2.MetricsException;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.MetricsSink;
import org.apache.hadoop.metrics2.MetricsTag;

public class Log4jMetricsSink implements MetricsSink {
  /* Configuration attribute names */
  private static final String HOST_PROPERTY = "host";
  private static final String PORT_PROPERTY = "port";
  private static final String TIMESTAMP = "timestamp";
  private static String CONTEXT = "context";
  private static final String CONTEXT_NAME = "contextName";
  private static final String RECORD_NAME = "recordName";
  protected String context = "HadoopMetrics";
  protected String host = "localhost";
  protected int port = 9095;
  protected Logger out = null;

  @Override
  public void init(SubsetConfiguration conf) {
    String host = conf.getString(HOST_PROPERTY);
    if (host != null) {
      this.host = host;
    }
    String port = conf.getString(PORT_PROPERTY);
    if (port != null) {
      this.port = Integer.parseInt(port);
    }
    String context = conf.getString(CONTEXT);
    if (context != null) {
      this.context = context;
    }

    PatternLayout layout = new PatternLayout("%d{ISO8601} %p %c: %m%n");

    org.apache.log4j.net.SocketAppender appender = new org.apache.log4j.net.SocketAppender(this.host, this.port);

    appender.setName("chukwa.metrics." + this.context);
    appender.setLayout(layout);

    Logger logger = Logger.getLogger("chukwa.metrics." + this.context);
    logger.setAdditivity(false);
    logger.addAppender(appender);
    appender.activateOptions();
    out = logger;
  }
  
  @Override
  @SuppressWarnings("unchecked")
  public void putMetrics(MetricsRecord record) {
      JSONObject json = new JSONObject();
      json.put(TIMESTAMP, Long.valueOf(record.timestamp()));
      json.put(CONTEXT_NAME, record.context());
      json.put(RECORD_NAME, record.name());
      for (MetricsTag tag : record.tags()) {
        json.put(tag.name(), tag.value());
      }
      for (Metric metric : record.metrics()) {
        json.put(metric.name(), metric.value());
      }
      out.info(json);
  }

  @Override
  public void flush() {
  }
}
