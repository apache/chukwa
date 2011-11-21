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
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

@Table(name="SystemMetrics",columnFamily="Ps")
public class Ps extends AbstractProcessor {
  static Logger log = Logger.getLogger(Ps.class);
  public static final String reduceType = "Ps";

  @Override
  protected void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    LogEntry log = new LogEntry(recordEntry);
    PsOutput ps = new PsOutput(log.getBody());
    for (HashMap<String, String> processInfo : ps.getProcessList()) {
      key = new ChukwaRecordKey();
      ChukwaRecord record = new ChukwaRecord();
      this.buildGenericRecord(record, null, log.getDate().getTime(), reduceType);
      for (Entry<String, String> entry : processInfo.entrySet()) {
        record.add(entry.getKey(), entry.getValue());
      }
      output.collect(key, record);
    }
  }

  public static class PsOutput {

    // processes info
    private ArrayList<HashMap<String, String>> recordList = new ArrayList<HashMap<String, String>>();

    public PsOutput(String psCmdOutput) throws InvalidPsRecord {
      if (psCmdOutput == null || psCmdOutput.length() == 0)
        return;

      String[] lines = psCmdOutput.split("[\n\r]+");

      // at least two lines
      if (lines.length < 2)
        return;

      // header
      ArrayList<String> header = new ArrayList<String>();
      Matcher matcher = Pattern.compile("[^ ^\t]+").matcher(lines[0]);
      while (matcher.find()) {
        header.add(matcher.group(0));
      }
      if (!header.get(header.size() - 1).equals("CMD")) {
        throw new InvalidPsRecord("CMD must be the last column");
      }

      // records
      boolean foundInitCmd = false;
      for (int line = 1; line < lines.length; line++) {
        HashMap<String, String> record = new HashMap<String, String>();
        recordList.add(record);

        matcher = Pattern.compile("[^ ^\t]+").matcher(lines[line]);
        for (int index = 0; index < header.size(); index++) {
          String key = header.get(index);
          matcher.find();
          if (!key.equals("CMD")) {
            String value = matcher.group(0);
            /**
             * For STARTED column, it could be in two formats: "MMM dd" or
             * "hh:mm:ss". If we use ' ' as the delimiter, we must read twice to
             * the date if it's with "MMM dd" format.
             */
            if (key.equals("STARTED")) {
              char c = value.charAt(0);
              if (c < '0' || c > '9') {
                matcher.find();
                value += matcher.group(0);
              }
            }
            record.put(key, value);
          } else {
            // reached the cmd part. all remains should be put
            // together as the command
            String value = lines[line].substring(matcher.start());
            record.put(key, value);
            if (!foundInitCmd)
              foundInitCmd = value.startsWith("init");
            break;
          }
        }
      }
      if (!foundInitCmd)
        throw new InvalidPsRecord("Did not find 'init' cmd");
    }

    public ArrayList<HashMap<String, String>> getProcessList() {
      return recordList;
    }
  }

  public static class InvalidPsRecord extends Exception {
    private static final long serialVersionUID = 1L;

    public InvalidPsRecord() {
    }

    public InvalidPsRecord(String arg0) {
      super(arg0);
    }

    public InvalidPsRecord(Throwable arg0) {
      super(arg0);
    }

    public InvalidPsRecord(String arg0, Throwable arg1) {
      super(arg0, arg1);
    }
  }
}
