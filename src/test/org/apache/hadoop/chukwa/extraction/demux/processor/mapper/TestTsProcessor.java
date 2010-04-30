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
import org.apache.hadoop.chukwa.extraction.demux.Demux;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import java.util.Map;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;

public class TestTsProcessor extends TestCase {

  private static String DATA_TYPE = "testDataType";
  private static String DATA_SOURCE = "testDataSource";

  JobConf jobConf = null;

  Date date = null;
  Date dateWithoutMillis = null;

  protected void setUp() throws Exception {
    jobConf = new JobConf();
    Demux.jobConf = jobConf;
    date = new Date();

    //if our format doesn't contain millis, then our final record date won't
    //have them either. let's create a sample date without millis for those tests
    //so our assertions will pass
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.MILLISECOND, 0);
    dateWithoutMillis = calendar.getTime();
  }

  public void testDefaultFormat() {
    String record = buildSampleSimpleRecord(date, "yyyy-MM-dd HH:mm:ss,SSS");
    doTest(date, record);
  }

  public void testCustomDefaultFormat() {
    jobConf.set("TsProcessor.default.time.format", "yyyy--MM--dd HH::mm::ss SSS");

    String record = buildSampleSimpleRecord(date, "yyyy--MM--dd HH::mm::ss SSS");
    doTest(date, record);
  }

  public void testCustomDefaultFormat2() {
    // this date format produces a date that longer than the format, since z
    // expands to something like PDT
    jobConf.set("TsProcessor.default.time.format", "yyyy--MM--dd HH::mm::ss SSS,z");

    String record = buildSampleSimpleRecord(date, "yyyy--MM--dd HH::mm::ss SSS,z");
    doTest(date, record);
  }

  public void testCustomDataTypeFormat() {
    jobConf.set("TsProcessor.time.format." + DATA_TYPE, "yyyy--MM--dd HH::mm::ss SSS");

    String record = buildSampleSimpleRecord(date, "yyyy--MM--dd HH::mm::ss SSS");
    doTest(date, record);
  }

  public void testCustomDefaultFormatWithCustomDataTypeFormat() {
    jobConf.set("TsProcessor.default.time.format", "yyyy/MM/dd HH:mm:ss SSS");
    jobConf.set("TsProcessor.time.format." + DATA_TYPE, "yyyy--MM--dd HH::mm::ss SSS");

    String record = buildSampleSimpleRecord(date, "yyyy--MM--dd HH::mm::ss SSS");
    doTest(date, record);
  }

  public void testCustomApacheDefaultFormat() {
    jobConf.set("TsProcessor.default.time.format", "dd/MMM/yyyy:HH:mm:ss Z");
    jobConf.set("TsProcessor.default.time.regex",
            "^(?:[\\d.]+) \\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [-+]\\d{4})\\] .*");


    String record = buildSampleApacheRecord(dateWithoutMillis, "dd/MMM/yyyy:HH:mm:ss Z");
    doTest(dateWithoutMillis, record);
  }

  public void testCustomApacheDataTypeFormat() {
    jobConf.set("TsProcessor.time.format." + DATA_TYPE, "dd/MMM/yyyy:HH:mm:ss Z");
    jobConf.set("TsProcessor.time.regex." + DATA_TYPE,
            "^(?:[\\d.]+) \\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} [-+]\\d{4})\\] .*");


    String record = buildSampleApacheRecord(dateWithoutMillis, "dd/MMM/yyyy:HH:mm:ss Z");
    doTest(dateWithoutMillis, record);
  }

  private static String buildSampleSimpleRecord(Date date, String dateFormat) {
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    return "" + sdf.format(date) + " some sample record data";
  }

  private static String buildSampleApacheRecord(Date date, String dateFormat) {
    SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
    return "10.10.182.49 [" + sdf.format(date) +
            "] \"\" 200 \"-\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10.5; en-US; rv:1.9.2.3) Gecko/20100401 Firefox/3.6.3\" \"some.site.com:8076\"";
  }

  public void doTest(Date date, String recordData) {
    ChunkBuilder cb = new ChunkBuilder();
    cb.addRecord(recordData.getBytes());

    Chunk chunk = cb.getChunk();
    chunk.setDataType(DATA_TYPE);
    chunk.setSource(DATA_SOURCE);

    ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord> output =
            new ChukwaTestOutputCollector<ChukwaRecordKey, ChukwaRecord>();

    TsProcessor p = new TsProcessor();
    p.reset(chunk);
    p.process(null, chunk, output, Reporter.NULL);

    ChukwaRecordKey key = buildKey(date, DATA_SOURCE, DATA_TYPE);
    Map<ChukwaRecordKey, ChukwaRecord> outputData = output.data;

    assertNotNull("No output data found.", outputData);
    assertEquals("Output data size not correct.", 1, outputData.size());

    ChukwaRecord record = outputData.get(key);
    assertNotNull("Output record not found.", record);
    assertEquals("Output record time not correct.", date.getTime(), record.getTime());
    assertEquals("Output record body not correct.", recordData,
            new String(record.getMapFields().get("body").get()));
  }

  private static ChukwaRecordKey buildKey(Date date, String dataSource, String dataType) {
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.set(Calendar.MINUTE, 0);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    ChukwaRecordKey key = new ChukwaRecordKey();
    key.setKey("" + calendar.getTimeInMillis() + "/" + dataSource + "/" + date.getTime());
    key.setReduceType(dataType);

    return key;
  }

}