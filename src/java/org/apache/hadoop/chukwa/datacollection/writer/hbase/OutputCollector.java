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

package org.apache.hadoop.chukwa.datacollection.writer.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

public class OutputCollector implements
    org.apache.hadoop.mapred.OutputCollector<ChukwaRecordKey, ChukwaRecord> {
  
  private List<Put> buffers;

  public OutputCollector() {
    buffers = new ArrayList<Put>();
  }
  
  @Override
  public void collect(ChukwaRecordKey key, ChukwaRecord value) throws IOException {
    StringBuffer s = new StringBuffer();
    String[] keyParts = key.getKey().split("/");
    s.append(keyParts[0]);
    s.append("-");
    s.append(keyParts[1]);

    for(String field : value.getFields()) {
        Put kv = new Put(s.toString().getBytes());
        kv.add(key.getReduceType().getBytes(), field.getBytes(), value.getTime(), value.getValue(field).getBytes());
        buffers.add(kv);
    }    
  }

  public List<Put> getKeyValues() {
    return buffers;
  }

  public void clear() {
    buffers.clear();
  }
  
}
