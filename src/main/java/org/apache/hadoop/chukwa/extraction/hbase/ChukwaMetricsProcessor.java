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

package org.apache.hadoop.chukwa.extraction.hbase;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Reporter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

public class ChukwaMetricsProcessor extends HadoopMetricsProcessor {  
  static Logger LOG = Logger.getLogger(ChukwaMetricsProcessor.class);
  
  public ChukwaMetricsProcessor() throws NoSuchAlgorithmException {
	super();
  }

  /**
   * Process cluster name and store in HBase.
   * 
   * @param chunk is a Chukwa data chunk
   * @param output is a list of Put operations
   * @param reporter is progress reporter
   * @throws Throwable if unable to send data
   */
  @Override
  public void process(Chunk chunk, ArrayList<Put> output, Reporter reporter)
      throws Throwable {
    this.output = output;
    this.reporter = reporter;
    this.chunk = chunk;
    this.primaryKeyHelper = chunk.getDataType();
    this.sourceHelper = chunk.getSource();
    String clusterName = chunk.getTag("cluster");
    reporter.putSource(primaryKeyHelper, sourceHelper);
    reporter.putClusterName(primaryKeyHelper, clusterName);
    parse(chunk.getData());
    addMeta();
  }

}
