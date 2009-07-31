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

package org.apache.hadoop.chukwa.extraction.archive;


import java.text.SimpleDateFormat;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.apache.log4j.Logger;

public class ChukwaArchiveDataTypeOutputFormat extends
    MultipleSequenceFileOutputFormat<ChukwaArchiveKey, ChunkImpl> {
  
  static final String GROUP_BY_CLUSTER_OPTION_NAME = "archive.groupByClusterName";
  static Logger log = Logger.getLogger(ChukwaArchiveDataTypeOutputFormat.class);
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy_MM_dd");
  boolean useClusterID;
  
  public RecordWriter<ChukwaArchiveKey,ChunkImpl>  getRecordWriter(FileSystem fs,
      JobConf job, String name, Progressable arg3) 
  throws java.io.IOException{

    log.info(GROUP_BY_CLUSTER_OPTION_NAME + " is " + job.get(GROUP_BY_CLUSTER_OPTION_NAME));
    useClusterID = "true".equals(job.get(GROUP_BY_CLUSTER_OPTION_NAME));

    return super.getRecordWriter(fs, job, name, arg3);
  }
  
  @Override
  protected String generateFileNameForKeyValue(ChukwaArchiveKey key,
      ChunkImpl chunk, String name) {

    if (log.isDebugEnabled()) {
      log.debug("ChukwaArchiveOutputFormat.fileName: "
          + sdf.format(key.getTimePartition()));
    }

    if(useClusterID) {
      String clusterID = RecordUtil.getClusterName(chunk);
      return clusterID + "/" + chunk.getDataType() + "_" + sdf.format(key.getTimePartition())
      + ".arc";
    } else
      return chunk.getDataType() + "_" + sdf.format(key.getTimePartition())
        + ".arc";
  }
}
