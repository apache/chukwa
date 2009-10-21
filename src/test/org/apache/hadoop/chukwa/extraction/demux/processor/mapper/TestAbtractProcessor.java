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


import junit.framework.TestCase;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkBuilder;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.RecordConstants;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TestAbtractProcessor extends TestCase {

  String[] data = { "dsjsjbsfjds\ndsafsfasd\n",
      "asdgHSAJGDGYDGGHAgd7364rt3478tc4\nhr473rt346t\n", "e	gqd	yeegyxuyexfg\n" };

  public void testParse() {

    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord(RecordConstants.escapeAllButLastRecordSeparator("\n", data[0])
        .getBytes());
    cb.addRecord(RecordConstants.escapeAllButLastRecordSeparator("\n", data[1])
        .getBytes());
    cb.addRecord(RecordConstants.escapeAllButLastRecordSeparator("\n", data[2])
        .getBytes());
    Chunk chunk = cb.getChunk();
    OutputCollector<ChukwaRecordKey, ChukwaRecord> output = new ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord>();
    TProcessor p = new TProcessor();
    p.data = data;
    p.process(null, chunk, output, null);
  }

}


class TProcessor extends AbstractProcessor {
  String[] data = null;
  int count = 0;

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter) {
    if (!recordEntry.equals(data[count])) {
      System.out.println("[" + recordEntry + "]");
      System.out.println("[" + data[count] + "]");
      throw new RuntimeException("not the same record");
    }
    count++;
  }

  public String getDataType() {
    // TODO Auto-generated method stub
    return null;
  }
}