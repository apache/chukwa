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

@Tables(annotations = { @Table(name = "Namenode", columnFamily = "summary"),
    @Table(name = "Namenode", columnFamily = "hdfs"),
    @Table(name = "Namenode", columnFamily = "rpc"),
    @Table(name = "Namenode", columnFamily = "jvm") })
public class NamenodeProcessor extends AbstractProcessor {
  static Map<String, Long> rateMap = new ConcurrentHashMap<String, Long>();

  static {
    long zero = 0L;
    rateMap.put("AddBlockOps", zero);
    rateMap.put("CreateFileOps", zero);
    rateMap.put("DeleteFileOps", zero);
    rateMap.put("FileInfoOps", zero);
    rateMap.put("FilesAppended", zero);
    rateMap.put("FilesCreated", zero);
    rateMap.put("FilesDeleted", zero);
    rateMap.put("FileInGetListingOps", zero);
    rateMap.put("FilesRenamed", zero);
    rateMap.put("GetBlockLocations", zero);
    rateMap.put("GetListingOps", zero);
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
    try {
      Logger log = Logger.getLogger(NamenodeProcessor.class);
      long timeStamp = Calendar.getInstance(TimeZone.getTimeZone("UTC"))
          .getTimeInMillis();

      final ChukwaRecord hdfs_overview = new ChukwaRecord();
      final ChukwaRecord hdfs_namenode = new ChukwaRecord();
      final ChukwaRecord namenode_jvm = new ChukwaRecord();
      final ChukwaRecord namenode_rpc = new ChukwaRecord();

      Map<String, ChukwaRecord> metricsMap = new HashMap<String, ChukwaRecord>() {
        private static final long serialVersionUID = 1L;
        {
          put("BlockCapacity", hdfs_overview);
          put("BlocksTotal", hdfs_overview);
          put("CapacityTotalGB", hdfs_overview);
          put("CapacityUsedGB", hdfs_overview);
          put("CapacityRemainingGB", hdfs_overview);
          put("CorruptBlocks", hdfs_overview);
          put("ExcessBlocks", hdfs_overview);
          put("FilesTotal", hdfs_overview);
          put("MissingBlocks", hdfs_overview);
          put("PendingDeletionBlocks", hdfs_overview);
          put("PendingReplicationBlocks", hdfs_overview);
          put("ScheduledReplicationBlocks", hdfs_overview);
          put("TotalLoad", hdfs_overview);
          put("UnderReplicatedBlocks", hdfs_overview);

          put("gcCount", namenode_jvm);
          put("gcTimeMillis", namenode_jvm);
          put("logError", namenode_jvm);
          put("logFatal", namenode_jvm);
          put("logInfo", namenode_jvm);
          put("logWarn", namenode_jvm);
          put("memHeapCommittedM", namenode_jvm);
          put("memHeapUsedM", namenode_jvm);
          put("threadsBlocked", namenode_jvm);
          put("threadsNew", namenode_jvm);
          put("threadsRunnable", namenode_jvm);
          put("threadsTerminated", namenode_jvm);
          put("threadsTimedWaiting", namenode_jvm);
          put("threadsWaiting", namenode_jvm);

          put("ReceivedBytes", namenode_rpc);
          put("RpcProcessingTime_avg_time", namenode_rpc);
          put("RpcProcessingTime_num_ops", namenode_rpc);
          put("RpcQueueTime_avg_time", namenode_rpc);
          put("RpcQueueTime_num_ops", namenode_rpc);
          put("SentBytes", namenode_rpc);
          put("rpcAuthorizationSuccesses", namenode_rpc);
          put("rpcAuthenticationFailures", namenode_rpc);
          put("rpcAuthenticationSuccesses", namenode_rpc);
        }
      };

      JSONObject obj = (JSONObject) JSONValue.parse(recordEntry);
      String ttTag = chunk.getTag("timeStamp");
      if (ttTag == null) {
        log.warn("timeStamp tag not set in JMX adaptor for namenode");
      } else {
        timeStamp = Long.parseLong(ttTag);
      }
      @SuppressWarnings("unchecked")
      Iterator<Map.Entry<String, ?>> keys = obj.entrySet().iterator();

      while (keys.hasNext()) {
        Map.Entry<String, ?> entry = keys.next();
        String key = entry.getKey();
        Object value = entry.getValue();
        String valueString = (value == null) ? "" : value.toString();

        // These metrics are string types with JSON structure. So we parse them
        // and get the count
        if (key.equals("LiveNodes") || key.equals("DeadNodes")
            || key.equals("DecomNodes") || key.equals("NameDirStatuses")) {
          JSONObject jobj = (JSONObject) JSONValue.parse(valueString);
          valueString = Integer.toString(jobj.size());
        }

        // Calculate rate for some of the metrics
        if (rateMap.containsKey(key)) {
          long oldValue = rateMap.get(key);
          long curValue = Long.parseLong(valueString);
          rateMap.put(key, curValue);
          long newValue = curValue - oldValue;
          if (newValue < 0) {
            log.error("NamenodeProcessor's rateMap might be reset or corrupted for metric "
                + key);
            newValue = 0L;
          }
          valueString = Long.toString(newValue);
        }

        // Check if metric belongs to one of the categories in metricsMap. If
        // not just write it in group Hadoop.HDFS.NameNode
        if (metricsMap.containsKey(key)) {
          ChukwaRecord rec = metricsMap.get(key);
          rec.add(key, valueString);
        } else {
          hdfs_namenode.add(key, valueString);
        }
      }
      buildGenericRecord(hdfs_overview, null, timeStamp, "summary");
      output.collect(key, hdfs_overview);
      buildGenericRecord(hdfs_namenode, null, timeStamp, "hdfs");
      output.collect(key, hdfs_namenode);
      buildGenericRecord(namenode_jvm, null, timeStamp, "jvm");
      output.collect(key, namenode_jvm);
      buildGenericRecord(namenode_rpc, null, timeStamp, "rpc");
      output.collect(key, namenode_rpc);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }

  }

}
