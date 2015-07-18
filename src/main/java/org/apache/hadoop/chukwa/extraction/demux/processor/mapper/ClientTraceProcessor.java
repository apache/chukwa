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

import java.io.IOException;
import java.net.InetAddress;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

@Table(name="Hadoop",columnFamily="ClientTrace")
public class ClientTraceProcessor extends AbstractProcessor {
  private static final String recordType = "ClientTrace";
  private final SimpleDateFormat sdf =
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  private final Matcher kvMatcher;
  private final Matcher idMatcher;
  private final Matcher ipMatcher;
  // extract date, source
  private final Pattern idPattern =
    Pattern.compile("^(.{23}).*clienttrace.*");
  // extract "key: value" pairs
  private final Pattern kvPattern =
    Pattern.compile("\\s+(\\w+):\\s+([^,]+)");
  private final Pattern ipPattern =
    Pattern.compile("[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+");

  public ClientTraceProcessor() {
    super();
    kvMatcher = kvPattern.matcher("");
    idMatcher = idPattern.matcher("");
    ipMatcher = ipPattern.matcher("");
  }

  public enum Locality {
    LOCAL("local"), INTRA("intra_rack"), INTER("inter_rack");
    String lbl;
    Locality(String lbl) {
      this.lbl = lbl;
    }
    public String getLabel() {
      return lbl;
    }
  };

  protected Locality getLocality(String src, String dst) throws Exception {
    if (null == src || null == dst) {
      throw new IOException("Missing src/dst");
    }
    ipMatcher.reset(src);
    if (!ipMatcher.find()) {
      throw new IOException("Could not find src");
    }
    byte[] srcIP = InetAddress.getByName(ipMatcher.group(0)).getAddress();
    ipMatcher.reset(dst);
    if (!ipMatcher.find()) {
      throw new IOException("Could not find dst");
    }
    byte[] dstIP = InetAddress.getByName(ipMatcher.group(0)).getAddress();
    for (int i = 0; i < 4; ++i) {
      if (srcIP[i] != dstIP[i]) {
        return (3 == i && (srcIP[i] & 0xC0) == (dstIP[i] & 0xC0))
          ? Locality.INTRA
          : Locality.INTER;
      }
    }
    return Locality.LOCAL;
  }

  @Override
  public void parse(String recordEntry,
      OutputCollector<ChukwaRecordKey,ChukwaRecord> output, Reporter reporter)
      throws Throwable {
    try {
      idMatcher.reset(recordEntry);
      long ms;
      long ms_fullresolution;
      if (idMatcher.find()) {
        ms = sdf.parse(idMatcher.group(1)).getTime();
        ms_fullresolution = ms;
      } else {
        throw new IOException("Could not find date/source");
      }
      kvMatcher.reset(recordEntry);
      if (!kvMatcher.find()) {
        throw new IOException("Failed to find record");
      }
      ChukwaRecord rec = new ChukwaRecord();
      do {
        rec.add(kvMatcher.group(1), kvMatcher.group(2));
      } while (kvMatcher.find());
      Locality loc = getLocality(rec.getValue("src"), rec.getValue("dest"));
      rec.add("locality", loc.getLabel());

      calendar.setTimeInMillis(ms);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      ms = calendar.getTimeInMillis();
      calendar.set(Calendar.MINUTE, 0);
      key.setKey(calendar.getTimeInMillis() + "/" + loc.getLabel() + "/" +
                 rec.getValue("op").toLowerCase() + "/" + ms);
      key.setReduceType("ClientTrace");
      rec.setTime(ms);

      rec.add(Record.tagsField, chunk.getTags());
      rec.add(Record.sourceField, chunk.getSource());
      rec.add(Record.applicationField, chunk.getStreamName());
      rec.add("actual_time",Long.toString(ms_fullresolution));
      output.collect(key, rec);

    } catch (ParseException e) {
      log.warn("Unable to parse the date in DefaultProcessor ["
          + recordEntry + "]", e);
      e.printStackTrace();
      throw e;
    } catch (IOException e) {
      log.warn("Unable to collect output in DefaultProcessor ["
          + recordEntry + "]", e);
      e.printStackTrace();
      throw e;
    }
  }

  public String getDataType() {
    return recordType;
  }
}
