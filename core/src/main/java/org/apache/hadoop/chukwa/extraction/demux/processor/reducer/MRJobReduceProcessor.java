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


import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class MRJobReduceProcessor implements ReduceProcessor {
  static Logger log = Logger.getLogger(MRJobReduceProcessor.class);

  @Override
  public String getDataType() {
    return MRJobReduceProcessor.class.getName();
  }

  @Override
  public void process(ChukwaRecordKey key, Iterator<ChukwaRecord> values,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter) {
    try {
      HashMap<String, String> data = new HashMap<String, String>();

      ChukwaRecord record = null;
      String[] fields = null;
      while (values.hasNext()) {
        record = values.next();
        fields = record.getFields();
        for (String field : fields) {
          data.put(field, record.getValue(field));
        }
      }

      // Extract initial time: SUBMIT_TIME
      long initTime = Long.parseLong(data.get("SUBMIT_TIME"));

      // Extract HodId
      // maybe use a regex to extract this and load it from configuration
      // JOBCONF=
      // "/user/xxx/mapredsystem/563976.xxx.yyy.com/job_200809062051_0001/job.xml"
      String jobConf = data.get("JOBCONF");
      int idx = jobConf.indexOf("mapredsystem/");
      idx += 13;
      int idx2 = jobConf.indexOf(".", idx);
      data.put("HodId", jobConf.substring(idx, idx2));

      ChukwaRecordKey newKey = new ChukwaRecordKey();
      newKey.setKey("" + initTime);
      newKey.setReduceType("MRJob");

      ChukwaRecord newRecord = new ChukwaRecord();
      newRecord.add(Record.tagsField, record.getValue(Record.tagsField));
      newRecord.setTime(initTime);
      newRecord.add(Record.tagsField, record.getValue(Record.tagsField));
      Iterator<Map.Entry<String, String>> it = data.entrySet().iterator();
      while (it.hasNext()) {
        Map.Entry<String, ?> entry = it.next();
        String field = entry.getKey();
        String value = entry.getValue().toString();
        newRecord.add(field, value);
      }

      output.collect(newKey, newRecord);
    } catch (IOException e) {
      log.warn("Unable to collect output in JobLogHistoryReduceProcessor ["
          + key + "]", e);
      e.printStackTrace();
    }

  }

}
