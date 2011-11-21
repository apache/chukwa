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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessor;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.TsProcessor;
import org.apache.hadoop.chukwa.extraction.demux.Demux;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Helper class used to create sequence files of Chukwa records
 */
public class CreateRecordFile {

   public static void makeTestSequenceFile(File inputFile,
                                           Path outputFile,
                                           String clusterName,
                                           String dataType,
                                           String streamName,
                                           MapProcessor processor) throws IOException {

     //initialize the output collector and the default processor
     MockOutputCollector collector = new MockOutputCollector();
     if (processor == null) processor = new TsProcessor();

     //initialize the sequence file writer
     Configuration conf = new Configuration();
     FileSystem fs = outputFile.getFileSystem(conf);
     FSDataOutputStream out = fs.create(outputFile);

     SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, out,
                                    ChukwaRecordKey.class, ChukwaRecord.class,
                                    SequenceFile.CompressionType.NONE, null);
     long lastSeqID = 0;
     String line;
     BufferedReader reader = new BufferedReader(new FileReader(inputFile));

     // for each line, create a chunk and an arckive key, pass it to the
     // processor, then write it to the sequence file.  
     while ((line = reader.readLine()) != null) {

       ChunkImpl chunk = new ChunkImpl(dataType, streamName,
         line.length()  + lastSeqID, line.getBytes(), null);
       lastSeqID += line.length();
       chunk.addTag("cluster=\"" + clusterName + "\"");

       ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();
       archiveKey.setTimePartition(System.currentTimeMillis());
       archiveKey.setDataType(chunk.getDataType());
       archiveKey.setStreamName(chunk.getStreamName());
       archiveKey.setSeqId(chunk.getSeqID());

       processor.process(archiveKey, chunk, collector, Reporter.NULL);
       seqFileWriter.append(collector.getChukwaRecordKey(),
                            collector.getChukwaRecord());
     }

     out.flush();
     out.close();
     seqFileWriter.close();
     reader.close();
   }

   private static class MockOutputCollector
           implements OutputCollector<ChukwaRecordKey, ChukwaRecord> {
     ChukwaRecordKey chukwaRecordKey;
     ChukwaRecord chukwaRecord;

     public void collect(ChukwaRecordKey chukwaRecordKey,
                         ChukwaRecord chukwaRecord) throws IOException {
       this.chukwaRecordKey = chukwaRecordKey;
       this.chukwaRecord = chukwaRecord;
     }

     public ChukwaRecordKey getChukwaRecordKey() { return chukwaRecordKey; }
     public ChukwaRecord getChukwaRecord() { return chukwaRecord; }
   }

   public static void main(String[] args) throws IOException,
                                                 ClassNotFoundException,
                                                 IllegalAccessException,
                                                 InstantiationException {
     if((args.length < 0 && args[0].contains("-h")) || args.length < 2) {
       usage();
     }

     File inputFile = new File(args[0]);
     Path outputFile = new Path(args[1]);
     String clusterName = "testClusterName";
     String dataType = "testDataType";
     String streamName = "testStreamName";               
     MapProcessor processor = new TsProcessor();
     Path confFile = null;

     if (args.length > 2) clusterName = args[2];
     if (args.length > 3) dataType = args[3];
     if (args.length > 4) streamName = args[4];

     if (args.length > 5) {
       Class clazz = null;
       try {
         clazz = Class.forName(args[5]);
       }
       catch (ClassNotFoundException e) {
         try {
           clazz = Class.forName(
                 "org.apache.hadoop.chukwa.extraction.demux.processor.mapper." + args[5]);
         }
         catch (Exception e2) {
           throw e;
         }
       }
       processor = (MapProcessor)clazz.newInstance();
     }

     if (args.length > 6) {
       confFile = new Path(args[6]);
       Demux.jobConf = new JobConf(confFile);
     }

     System.out.println("Creating sequence file using the following input:");
     System.out.println("inputFile  : " + inputFile);
     System.out.println("outputFile : " + outputFile);
     System.out.println("clusterName: " + clusterName);
     System.out.println("dataType   : " + dataType);
     System.out.println("streamName : " + streamName);
     System.out.println("processor  : " + processor.getClass().getName());
     System.out.println("confFile   : " + confFile);

     makeTestSequenceFile(inputFile, outputFile, clusterName, dataType, streamName, processor);

     System.out.println("Done");
   }

   public static void usage() {
     System.out.println("Usage: java " + TempFileUtil.class.toString().split(" ")[1] + " <inputFile> <outputFile> [<clusterName> <dataType> <streamName> <processorClass> [confFile]]");
     System.out.println("Description: Takes a plain text input file and generates a Hadoop sequence file contaning ChukwaRecordKey,ChukwaRecord entries");
     System.out.println("Parameters: inputFile      - Text input file to read");
     System.out.println("            outputFile     - Sequence file to create");
     System.out.println("            clusterName    - Cluster name to use in the records");
     System.out.println("            dataType       - Data type to use in the records");
     System.out.println("            streamName     - Stream name to use in the records");
     System.out.println("            processorClass - Processor class to use. Defaults to TsProcessor");
     System.out.println("            confFile       - File to use to create the JobConf");
     System.exit(0);
   }
}
