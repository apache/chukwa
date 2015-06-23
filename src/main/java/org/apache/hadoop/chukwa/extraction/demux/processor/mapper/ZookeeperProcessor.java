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

import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

@Tables(annotations = { @Table(name = "Zookeeper", columnFamily = "zk") })
public class ZookeeperProcessor extends AbstractProcessor {

  static Map<String, Long> rateMap = new ConcurrentHashMap<String, Long>();
  static {
    long zero = 0L;
    rateMap.put("PacketsSent", zero);
    rateMap.put("PacketsReceived", zero);
  }

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    Logger log = Logger.getLogger(ZookeeperProcessor.class);
    long timeStamp = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        .getTimeInMillis();
    final ChukwaRecord record = new ChukwaRecord();

    Map<String, ChukwaRecord> metricsMap = new HashMap<String, ChukwaRecord>() {
      private static final long serialVersionUID = 1L;

      {
        put("MinRequestLatency", record);
        put("AvgRequestLatency", record);
        put("MaxRequestLatency", record);
        put("PacketsReceived", record);
        put("PacketsSent", record);
        put("OutstandingRequests", record);
        put("NodeCount", record);
        put("WatchCount", record);
      }
    };
    try {
      JSONObject obj = (JSONObject) JSONValue.parse(recordEntry);
      String ttTag = chunk.getTag("timeStamp");
      if (ttTag == null) {
        log.warn("timeStamp tag not set in JMX adaptor for zookeeper");
      } else {
        timeStamp = Long.parseLong(ttTag);
      }
      Iterator<String> keys = ((JSONObject) obj).keySet().iterator();

      while (keys.hasNext()) {
        String key = keys.next();
        Object value = obj.get(key);
        String valueString = value == null ? "" : value.toString();

        if (metricsMap.containsKey(key)) {
          ChukwaRecord rec = metricsMap.get(key);
          rec.add(key, valueString);
        }
      }

      buildGenericRecord(record, null, timeStamp, "zk");
      output.collect(key, record);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
}
