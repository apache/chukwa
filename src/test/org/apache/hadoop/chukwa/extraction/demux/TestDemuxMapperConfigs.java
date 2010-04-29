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

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.ChunkBuilder;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.ChukwaTestOutputCollector;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import junit.framework.TestCase;


import java.io.IOException;

/**
 * Tests that settings related to the Demux mapper do what they should.
 */
public class TestDemuxMapperConfigs extends TestCase {

  public static  String SAMPLE_RECORD_DATA = "sampleRecordData";

  public void testSetDefaultMapProcessor() throws IOException {
    Mapper<ChukwaArchiveKey, ChunkImpl, ChukwaRecordKey, ChukwaRecord> mapper =
            new Demux.MapClass();

    JobConf conf = new JobConf();
    conf.set("chukwa.demux.mapper.default.processor",
             "org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MockMapProcessor");
    mapper.configure(conf);

    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord(SAMPLE_RECORD_DATA.getBytes());
    ChunkImpl chunk = (ChunkImpl)cb.getChunk();

    ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord> output =
            new ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord>();

    mapper.map(new ChukwaArchiveKey(), chunk, output, Reporter.NULL);
    ChukwaRecordKey recordKey = new ChukwaRecordKey("someReduceType", SAMPLE_RECORD_DATA);

    assertEquals("MockMapProcessor never invoked - no records found", 1, output.data.size());
    assertNotNull("MockMapProcessor never invoked", output.data.get(recordKey));
  }
}
