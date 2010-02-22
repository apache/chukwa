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

package org.apache.hadoop.chukwa.util;

import junit.framework.TestCase;

import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.net.InetAddress;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Calendar;

import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.TsProcessor;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class TestCreateRecordFile extends TestCase {
  private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  private Calendar calendar = Calendar.getInstance();

  public void testWriteSequenceFile() throws IOException, ParseException {
    String outputDir = System.getProperty("test.build.data", "/tmp");

    //input configs
    String datadir = System.getenv("CHUKWA_DATA_DIR");
    if(datadir == null)
      datadir = "test/samples";
    else 
      datadir = datadir + File.separator + "log";
    File inputFile = new File( datadir+ File.separator + "ClientTrace.log");
    Path outputFile = new Path(outputDir + "/" + this.getClass().getName() + "/ClientTrace.evt");
    String clusterName = "testClusterName";
    String dataType = "testDataType";
    String streamName = "testStreamName";
    MapProcessor processor = new TsProcessor();

    //create the sequence file
    CreateRecordFile.makeTestSequenceFile(inputFile, outputFile, clusterName,
                                          dataType, streamName, processor);
    //read the output file
    ChukwaRecordKey key = new ChukwaRecordKey();
    ChukwaRecord record = new ChukwaRecord();

    Configuration conf = new Configuration();
    FileSystem fs = outputFile.getFileSystem(conf);
    SequenceFile.Reader sequenceReader = new SequenceFile.Reader(fs, outputFile, conf);

    //read the input file to assert
    BufferedReader inputReader = new BufferedReader(new FileReader(inputFile));

    String expectedHostname = InetAddress.getLocalHost().getHostName();

    //Read input and output back comparing each
    int i = 0;
    while (sequenceReader.next(key, record)) {
      String line = inputReader.readLine();
      assertNotNull("Sequence file contains more records than input file", line);

      long expectedTime = sdf.parse(line.substring(0,23)).getTime();
      calendar.setTimeInMillis(expectedTime);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);

      String expectedKey = calendar.getTimeInMillis() + "/" +
                           expectedHostname + "/" + expectedTime;
      String expectedTags = "cluster=\"" + clusterName + "\"";

      //assert key
      assertEquals("Invalid key found for record " + i,   expectedKey, key.getKey());
      assertEquals("Invalid dataType found for record " + i, dataType, key.getReduceType());

      //assert record
      assertEquals("Invalid record time for record " + i, expectedTime, record.getTime());
      assertEquals("Invalid body for record " + i, line, record.getValue("body"));
      assertEquals("Invalid capp for record " + i, streamName, record.getValue("capp"));
      assertEquals("Invalid csource for record " + i, expectedHostname, record.getValue("csource"));
      assertEquals("Invalid ctags for record " + i, expectedTags , record.getValue("ctags").trim());

      i++;
    }

    sequenceReader.close();
    inputReader.close();
  }

}