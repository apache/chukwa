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
package org.apache.hadoop.chukwa.extraction.demux.processor;


import java.io.IOException;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ChukwaOutputCollector implements
    OutputCollector<ChukwaRecordKey, ChukwaRecord> {
  private OutputCollector<ChukwaRecordKey, ChukwaRecord> outputCollector = null;
  private Reporter reporter = null;
  private String groupName = null;

  public ChukwaOutputCollector(
                               String groupName,
                               OutputCollector<ChukwaRecordKey, ChukwaRecord> outputCollector,
                               Reporter reporter) {
    this.reporter = reporter;
    this.outputCollector = outputCollector;
    this.groupName = groupName;
  }

  @Override
  public void collect(ChukwaRecordKey key, ChukwaRecord value)
      throws IOException {
    this.outputCollector.collect(key, value);
    reporter.incrCounter(groupName, "total records", 1);
    reporter.incrCounter(groupName, key.getReduceType() + " records", 1);
  }

}
