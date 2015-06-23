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

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

@Tables(annotations = { @Table(name = "Datanode", columnFamily = "dn"),
    @Table(name = "Datanode", columnFamily = "jvm"),
    @Table(name = "Datanode", columnFamily = "rpc") })
public class DatanodeProcessor extends AbstractProcessor {

  static Map<String, Long> rateMap = new ConcurrentHashMap<String, Long>();

  static {
    long zero = 0L;
    rateMap.put("blocks_verified", zero);
    rateMap.put("blocks_written", zero);
    rateMap.put("blocks_read", zero);
    rateMap.put("bytes_written", zero);
    rateMap.put("bytes_read", zero);
    rateMap.put("heartBeats_num_ops", zero);
    rateMap.put("SentBytes", zero);
    rateMap.put("ReceivedBytes", zero);
    rateMap.put("rpcAuthorizationSuccesses", zero);
    rateMap.put("rpcAuthorizationFailures", zero);
    rateMap.put("RpcQueueTime_num_ops", zero);
    rateMap.put("RpcProcessingTime_num_ops", zero);
    rateMap.put("gcCount", zero);
  }

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    Logger log = Logger.getLogger(DatanodeProcessor.class);
    long timeStamp = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
        .getTimeInMillis();

    final ChukwaRecord hdfs_datanode = new ChukwaRecord();
    final ChukwaRecord datanode_jvm = new ChukwaRecord();
    final ChukwaRecord datanode_rpc = new ChukwaRecord();

    Map<String, ChukwaRecord> metricsMap = new HashMap<String, ChukwaRecord>() {
      private static final long serialVersionUID = 1L;

      {
        put("blocks_verified", hdfs_datanode);
        put("blocks_written", hdfs_datanode);
        put("blocks_read", hdfs_datanode);
        put("blocks_replicated", hdfs_datanode);
        put("blocks_removed", hdfs_datanode);
        put("bytes_written", hdfs_datanode);
        put("bytes_read", hdfs_datanode);
        put("heartBeats_avg_time", hdfs_datanode);
        put("heartBeats_num_ops", hdfs_datanode);

        put("gcCount", datanode_jvm);
        put("gcTimeMillis", datanode_jvm);
        put("logError", datanode_jvm);
        put("logFatal", datanode_jvm);
        put("logInfo", datanode_jvm);
        put("logWarn", datanode_jvm);
        put("memHeapCommittedM", datanode_jvm);
        put("memHeapUsedM", datanode_jvm);
        put("threadsBlocked", datanode_jvm);
        put("threadsNew", datanode_jvm);
        put("threadsRunnable", datanode_jvm);
        put("threadsTerminated", datanode_jvm);
        put("threadsTimedWaiting", datanode_jvm);
        put("threadsWaiting", datanode_jvm);

        put("ReceivedBytes", datanode_rpc);
        put("RpcProcessingTime_avg_time", datanode_rpc);
        put("RpcProcessingTime_num_ops", datanode_rpc);
        put("RpcQueueTime_avg_time", datanode_rpc);
        put("RpcQueueTime_num_ops", datanode_rpc);
        put("SentBytes", datanode_rpc);
        put("rpcAuthorizationSuccesses", datanode_rpc);
      }
    };
    try {
      JSONObject obj = (JSONObject) JSONValue.parse(recordEntry);
      String ttTag = chunk.getTag("timeStamp");
      if (ttTag == null) {
        log.warn("timeStamp tag not set in JMX adaptor for datanode");
      } else {
        timeStamp = Long.parseLong(ttTag);
      }
      Iterator<String> keys = obj.keySet().iterator();

      while (keys.hasNext()) {
        String key = keys.next();
        Object value = obj.get(key);
        String valueString = value == null ? "" : value.toString();

        // Calculate rate for some of the metrics
        if (rateMap.containsKey(key)) {
          long oldValue = rateMap.get(key);
          long curValue = Long.parseLong(valueString);
          rateMap.put(key, curValue);
          long newValue = curValue - oldValue;
          if (newValue < 0) {
            log.error("DatanodeProcessor's rateMap might be reset or corrupted for metric "
                + key);
            newValue = 0L;
          }
          valueString = Long.toString(newValue);
        }

        if (metricsMap.containsKey(key)) {
          ChukwaRecord rec = metricsMap.get(key);
          rec.add(key, valueString);
        }
      }
      buildGenericRecord(hdfs_datanode, null, timeStamp, "dn");
      output.collect(key, hdfs_datanode);
      buildGenericRecord(datanode_jvm, null, timeStamp, "jvm");
      output.collect(key, datanode_jvm);
      buildGenericRecord(datanode_rpc, null, timeStamp, "rpc");
      output.collect(key, datanode_rpc);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }
}
