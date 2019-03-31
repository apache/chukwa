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
package org.apache.hadoop.chukwa.datacollection.sender;

import junit.framework.TestCase;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.*;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.sender.ChukwaHttpSender.CommitListEntry;

public class TestAcksOnFailure extends TestCase {
  public void testNoCollector() {
    
    Configuration conf = new Configuration();
    conf.setInt("chukwaAgent.sender.retries", 3);
    conf.setInt("chukwaAgent.sender.retryInterval", 1000);
    
    ChukwaHttpSender send = new ChukwaHttpSender(conf);
    ArrayList<String> collectors = new ArrayList<String>();
    collectors.add("http://somehost.invalid/chukwa");
    send.setCollectors(new RetryListOfCollectors(collectors, conf));
    
    byte[] data = "sometestdata".getBytes();
    Adaptor a = new FileTailingAdaptor();
    ChunkImpl ci = new ChunkImpl("testtype", "sname", data.length, data, a);
    ArrayList<Chunk> toSend = new ArrayList<Chunk>();
    toSend.add(ci);
    try {
      List<CommitListEntry> resp = send.send(toSend);
      assertTrue(resp.size() == 0);
    } catch(Exception e) {
      
    }
  }

}
