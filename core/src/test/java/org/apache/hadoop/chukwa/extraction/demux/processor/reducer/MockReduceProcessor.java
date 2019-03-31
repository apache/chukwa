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
package org.apache.hadoop.chukwa.extraction.demux.processor.reducer;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.util.Iterator;
import java.io.IOException;

public class MockReduceProcessor implements ReduceProcessor {

  public String getDataType() {
    return "MockDataType";
  }

  public void process(ChukwaRecordKey key, Iterator<ChukwaRecord> values,
                      OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
                      Reporter reporter) {
    ChukwaRecord record = new ChukwaRecord();
    record.add("MockReduceProcessorKey", "MockReduceProcessorValue");

    try {
      output.collect(key, record);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}