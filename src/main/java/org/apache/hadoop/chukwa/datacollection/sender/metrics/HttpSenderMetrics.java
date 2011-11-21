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
package org.apache.hadoop.chukwa.datacollection.sender.metrics;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;

public class HttpSenderMetrics implements Updater {

  public MetricsRegistry registry = new MetricsRegistry();
  private MetricsRecord metricsRecord;
  private HttpSenderActivityMBean mbean;
  
  
  public MetricsTimeVaryingInt collectorRollover =
    new MetricsTimeVaryingInt("collectorRollover", registry,"number of collector rollovert");
  
  public MetricsTimeVaryingInt httpPost =
    new MetricsTimeVaryingInt("httpPost", registry,"number of HTTP post");
  
  public MetricsTimeVaryingInt httpException =
    new MetricsTimeVaryingInt("httpException", registry,"number of HTTP Exception");

  public MetricsTimeVaryingInt httpThrowable =
    new MetricsTimeVaryingInt("httpThrowable", registry,"number of HTTP Throwable exception");
  
  public MetricsTimeVaryingInt httpTimeOutException =
    new MetricsTimeVaryingInt("httpTimeOutException", registry,"number of HTTP TimeOutException");
  
  /** Creates a new instance of HttpSenderMetrics */
  public HttpSenderMetrics(String processName, String recordName) {
      MetricsContext context = MetricsUtil.getContext(processName);
      metricsRecord = MetricsUtil.createRecord(context, recordName);
      metricsRecord.setTag("process", processName);
      mbean = new HttpSenderActivityMBean(registry, recordName);
      context.registerUpdater(this);
  }


  /**
   * Since this object is a registered updater, this method will be called
   * periodically, e.g. every 5 seconds.
   */
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : registry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public void shutdown() {
    if (mbean != null)
      mbean.shutdown();
  }

}
