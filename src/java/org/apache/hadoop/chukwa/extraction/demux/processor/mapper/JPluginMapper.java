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

package org.apache.hadoop.chukwa.extraction.demux.processor.mapper;

import java.util.Map.Entry;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.inputtools.jplugin.ChukwaMetrics;
import org.apache.hadoop.chukwa.inputtools.jplugin.GenericChukwaMetricsList;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class JPluginMapper extends AbstractProcessor {
  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
      Reporter reporter) throws Throwable {
    LogEntry entry = new LogEntry(recordEntry);
    String xml = entry.getBody();
    GenericChukwaMetricsList metricsList = new GenericChukwaMetricsList();
    metricsList.fromXml(xml);
    String recType = metricsList.getRecordType();
    long timestamp = metricsList.getTimestamp();
    for (ChukwaMetrics metrics : metricsList.getMetricsList()) {
      key = new ChukwaRecordKey();
      ChukwaRecord record = new ChukwaRecord();
      this.buildGenericRecord(record, null, -1l, recType);
      record.setTime(timestamp);
      key.setKey(getKey(timestamp, metrics.getKey()));
      record.add("key", metrics.getKey());
      for (Entry<String, String> attr : metrics.getAttributes().entrySet()) {
        record.add(attr.getKey(), attr.getValue());
      }
      output.collect(key, record);
    }
  }

  private String getKey(long ts, String metricsKey) {
    long unit = 60 * 60 * 1000;
    long rounded = (ts / unit) * unit;
    return rounded + "/" + metricsKey + "/" + ts;
  }
}
