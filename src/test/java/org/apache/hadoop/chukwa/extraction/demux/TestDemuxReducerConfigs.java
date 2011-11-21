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

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.ChukwaTestOutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reducer;
import junit.framework.TestCase;

import java.io.IOException;

/**
 * Tests that settings related to the Demux mapper do what they should.
 */
public class TestDemuxReducerConfigs extends TestCase {

  public static  String SAMPLE_RECORD_DATA = "sampleRecordData";

  public void testSetDefaultReducerProcessor() throws IOException {
    Reducer<ChukwaRecordKey, ChukwaRecord, ChukwaRecordKey, ChukwaRecord> reducer =
            new Demux.ReduceClass();

    JobConf conf = new JobConf();
    conf.set("chukwa.demux.reducer.default.processor", "MockReduceProcessor");
    reducer.configure(conf);

    ChukwaRecordKey key = new ChukwaRecordKey("someReduceType", "someKey");
    ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord> output =
            new ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord>();

    reducer.reduce(key, null, output, Reporter.NULL);

    assertEquals("MockReduceProcessor never invoked - no records found", 1, output.data.size());
    assertNotNull("MockReduceProcessor never invoked", output.data.get(key));
    assertEquals("MockReduceProcessor never invoked - key value incorrect",
            "MockReduceProcessorValue",
            output.data.get(key).getValue("MockReduceProcessorKey"));
  }
}