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
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Iterator;

import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

public class ClientTrace implements ReduceProcessor {

  static final Logger log = Logger.getLogger(ClientTrace.class);

  @Override
  public String getDataType() {
    return this.getClass().getName();
  }

  @Override
  public void process(ChukwaRecordKey key,
            Iterator<ChukwaRecord> values,
            OutputCollector<ChukwaRecordKey, ChukwaRecord> output,
            Reporter reporter) {
    try {
      long bytes = 0L;
      ChukwaRecord rec = null;
      while (values.hasNext()) {
        /* aggregate bytes for current key */
        rec = values.next();
        bytes += Long.valueOf(rec.getValue("bytes"));
        
        /* output raw values to different data type for uses which
         * require detailed per-operation data */
        ChukwaRecordKey detailed_key = new ChukwaRecordKey();
        String [] k = key.getKey().split("/");
        String full_timestamp = null;
        full_timestamp = rec.getValue("actual_time");
        detailed_key.setReduceType("ClientTraceDetailed");
        detailed_key.setKey(k[0]+"/"+k[1]+"_"+k[2]+"/"+full_timestamp);
        output.collect(detailed_key, rec);
      }
      if (null == rec) {
        return;
      }
      ChukwaRecord emit = new ChukwaRecord();
      emit.add(Record.tagsField, rec.getValue(Record.tagsField));
      emit.add(Record.sourceField, "undefined"); // TODO
      emit.add(Record.applicationField, rec.getValue(Record.applicationField));

      String[] k = key.getKey().split("/");
      emit.add(k[1] + "_" + k[2], String.valueOf(bytes));
      emit.setTime(Long.valueOf(k[3]));
      output.collect(key, emit);

    } catch (IOException e) {
      log.warn("Unable to collect output in SystemMetricsReduceProcessor [" + key + "]", e);
    }
  }
}
