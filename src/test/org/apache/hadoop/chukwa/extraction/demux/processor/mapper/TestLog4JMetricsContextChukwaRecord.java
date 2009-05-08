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

import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.Log4JMetricsContextProcessor.Log4JMetricsContextChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;

public class TestLog4JMetricsContextChukwaRecord extends TestCase {
  private static String[] chukwaQueueLog = { 
      "2009-05-06 00:00:21,982 INFO chukwa.metrics.chunkQueue: {\"removedChunk\":1,\"recordName\":\"chunkQueue\",\"queueSize\":94,\"chukwa_timestamp\":1241568021982,\"removedChunk_raw\":0,\"dataSize\":10373608,\"fullQueue\":1,\"addedChunk_rate\":0,\"addedChunk_raw\":0,\"period\":60,\"addedChunk\":95,\"hostName\":\"test.com\",\"removedChunk_rate\":0,\"contextName\":\"chunkQueue\"}",
      "2009-05-06 00:01:21,981 INFO chukwa.metrics.chunkQueue: {\"removedChunk\":1,\"recordName\":\"chunkQueue\",\"queueSize\":94,\"chukwa_timestamp\":1241568081981,\"removedChunk_raw\":0,\"dataSize\":10373608,\"fullQueue\":1,\"addedChunk_rate\":0,\"addedChunk_raw\":0,\"period\":60,\"addedChunk\":95,\"hostName\":\"test.com\",\"removedChunk_rate\":0,\"contextName\":\"chunkQueue\"}",
      "2009-05-06 00:02:21,982 INFO chukwa.metrics.chunkQueue: {\"removedChunk\":1,\"recordName\":\"chunkQueue\",\"queueSize\":94,\"chukwa_timestamp\":1241568141982,\"removedChunk_raw\":0,\"dataSize\":10373608,\"fullQueue\":1,\"addedChunk_rate\":0,\"addedChunk_raw\":0,\"period\":60,\"addedChunk\":95,\"hostName\":\"test.com\",\"removedChunk_rate\":0,\"contextName\":\"chunkQueue\"}",
  };
  
  private static String[] chukwaAgentLog = {
    "2009-05-06 23:33:35,213 INFO chukwa.metrics.chukwaAgent: {\"addedAdaptor_rate\":0,\"addedAdaptor_raw\":0,\"recordName\":\"chukwaAgent\",\"chukwa_timestamp\":1241652815212,\"removedAdaptor_rate\":0,\"removedAdaptor\":0,\"period\":60,\"adaptorCount\":4,\"removedAdaptor_raw\":0,\"process\":\"ChukwaAgent\",\"addedAdaptor\":4,\"hostName\":\"test.com\",\"contextName\":\"chukwaAgent\"}",
    "2009-05-06 23:34:35,211 INFO chukwa.metrics.chukwaAgent: {\"addedAdaptor_rate\":0,\"addedAdaptor_raw\":0,\"recordName\":\"chukwaAgent\",\"chukwa_timestamp\":1241652875211,\"removedAdaptor_rate\":0,\"removedAdaptor\":0,\"period\":60,\"adaptorCount\":4,\"removedAdaptor_raw\":0,\"process\":\"ChukwaAgent\",\"addedAdaptor\":4,\"hostName\":\"test.com\",\"contextName\":\"chukwaAgent\"}",
    "2009-05-06 23:35:35,212 INFO chukwa.metrics.chukwaAgent: {\"addedAdaptor_rate\":0,\"addedAdaptor_raw\":0,\"recordName\":\"chukwaAgent\",\"chukwa_timestamp\":1241652935212,\"removedAdaptor_rate\":0,\"removedAdaptor\":0,\"period\":60,\"adaptorCount\":4,\"removedAdaptor_raw\":0,\"process\":\"ChukwaAgent\",\"addedAdaptor\":4,\"hostName\":\"test.com\",\"contextName\":\"chukwaAgent\"}",
    "2009-05-06 23:39:35,215 INFO chukwa.metrics.chukwaAgent: {\"addedAdaptor_rate\":0,\"addedAdaptor_raw\":0,\"recordName\":\"chukwaAgent\",\"chukwa_timestamp\":1241653175214,\"removedAdaptor_rate\":0,\"removedAdaptor\":0,\"period\":60,\"adaptorCount\":4,\"removedAdaptor_raw\":0,\"process\":\"ChukwaAgent\",\"addedAdaptor\":4,\"hostName\":\"test.com\",\"contextName\":\"CA\"}",
  };

  public void testLog4JMetricsContextChukwaRecord() throws Throwable {
    {
      Log4JMetricsContextChukwaRecord rec = new Log4JMetricsContextChukwaRecord(chukwaQueueLog[0]);
      ChukwaRecord chukwaRecord = rec.getChukwaRecord();
      assertEquals("chunkQueue", rec.getRecordType());
      assertEquals("1241568021982", chukwaRecord.getValue("chukwa_timestamp"));
      assertEquals((1241568021982l/60000)*60000, rec.getTimestamp());
      assertEquals("94", chukwaRecord.getValue("queueSize"));
    }
    
    {
      Log4JMetricsContextChukwaRecord rec = new Log4JMetricsContextChukwaRecord(chukwaAgentLog[3]);
      assertEquals("CA_chukwaAgent", rec.getRecordType());
      assertEquals(1241653175214l/60000*60000, rec.getTimestamp());
    }
  }

}
