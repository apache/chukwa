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
package org.apache.hadoop.chukwa.datacollection.agent.metrics;

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsIntValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingInt;

public class ChunkQueueMetrics implements Updater {

  public MetricsRegistry registry = new MetricsRegistry();
  private MetricsRecord metricsRecord;
  private ChunkQueueActivityMBean mbean;


  public MetricsIntValue queueSize =
    new MetricsIntValue("queueSize", registry,"Queue size");
  
  public MetricsLongValue dataSize =
    new MetricsLongValue("dataSize", registry,"Data size");
  
  public MetricsTimeVaryingInt addedChunk =
    new MetricsTimeVaryingInt("addedChunk", registry,"number of added chunk");
  
  public MetricsTimeVaryingInt removedChunk =
    new MetricsTimeVaryingInt("removedChunk", registry,"number of removed chunk");
  
  public MetricsIntValue fullQueue =
    new MetricsIntValue("fullQueue", registry,"Queue is full");
  
  
  /** Creates a new instance of QueueMetrics */
  public ChunkQueueMetrics(String processName, String recordName) {
      MetricsContext context = MetricsUtil.getContext(processName);
      metricsRecord = MetricsUtil.createRecord(context, recordName);
      mbean = new ChunkQueueActivityMBean(registry, recordName);
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
