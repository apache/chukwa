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

package org.apache.hadoop.chukwa.extraction.demux;


import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.log4j.Logger;

public class ChukwaRecordPartitioner<K, V> implements
    Partitioner<ChukwaRecordKey, ChukwaRecord> {
  static Logger log = Logger.getLogger(ChukwaRecordPartitioner.class);

  public void configure(JobConf arg0) {
  }

  public int getPartition(
      org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey key,
      org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord record,
      int numReduceTasks) {
    if (log.isDebugEnabled()) {

      log
          .debug("Partitioner key: ["
              + key.getReduceType()
              + "] - Reducer:"
              + ((key.getReduceType().hashCode() & Integer.MAX_VALUE) % numReduceTasks));
    }
    String hashkey = key.getReduceType();
    if(key.getKey().startsWith(CHUKWA_CONSTANT.INCLUDE_KEY_IN_PARTITIONER)) hashkey = key.getReduceType()+"#"+key.getKey();
    return (hashkey.hashCode() & Integer.MAX_VALUE)
        % numReduceTasks;
  }

}
