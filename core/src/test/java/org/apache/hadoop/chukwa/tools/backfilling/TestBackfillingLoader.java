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
package org.apache.hadoop.chukwa.tools.backfilling;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.chukwa.datacollection.writer.parquet.ChukwaAvroSchema;
import org.apache.hadoop.chukwa.validationframework.util.MD5;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;

public class TestBackfillingLoader extends TestCase{

  private String cluster = "chukwa";
  
  public void testBackfillingLoaderWithCharFileTailingAdaptorUTF8NewLineEscaped() {
    String tmpDir = System.getProperty("test.build.data", "/tmp");
    long ts = System.currentTimeMillis();
    String dataDir = tmpDir + "/TestBackfillingLoader_" + ts;

    Configuration conf = new Configuration();
    conf.set("writer.hdfs.filesystem", "file:///");
    conf.set("chukwaCollector.outputDir", dataDir  + "/log/");
    conf.set("chukwaCollector.rotateInterval", "" + (Integer.MAX_VALUE -1));
    
    String cluster = "chukwa";
    String machine = "machine_" + ts;
    String adaptorName = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped";
    String recordType = "MyRecordType_" + ts;
    
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      
      File in1Dir = new File(dataDir + "/input");
      in1Dir.mkdirs();
      int lineCount = 107;
      File inputFile = makeTestFile(dataDir + "/input/in1.txt",lineCount);
      long size = inputFile.length();
      
      String logFile = inputFile.getAbsolutePath();
      System.out.println("Output:" + logFile);
      System.out.println("File:" + inputFile.length());
      BackfillingLoader loader = new BackfillingLoader(conf,cluster,machine,adaptorName,recordType,logFile);
      loader.process();
      
      File finalOutputFile = new File(dataDir + "/input/in1.txt.sav");
      
      Assert.assertTrue(inputFile.exists() == false);
      Assert.assertTrue(finalOutputFile.exists() == true);
      
      String doneFile = null;
      File directory = new File(dataDir  + "/log/");
      String[] files = directory.list();
      for(String file: files) {
        if ( file.endsWith(".done") ){
          doneFile = dataDir  + "/log/" + file;
          break;
        }
      }
      
      long seqId = validateDataSink(fs,conf,doneFile,finalOutputFile,
          cluster, recordType,  machine, logFile);
      Assert.assertTrue(seqId == size);
      
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
    try {
      FileUtils.deleteDirectory(new File(dataDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void testBackfillingLoaderWithFileAdaptor() {
    String tmpDir = System.getProperty("test.build.data", "/tmp");
    long ts = System.currentTimeMillis();
    String dataDir = tmpDir + "/TestBackfillingLoader_" + ts;

    Configuration conf = new Configuration();
    conf.set("writer.hdfs.filesystem", "file:///");
    conf.set("chukwaCollector.outputDir", dataDir  + "/log/");
    conf.set("chukwaCollector.rotateInterval", "" + (Integer.MAX_VALUE -1));
    
    String machine = "machine_" + ts;
    String adaptorName = "org.apache.hadoop.chukwa.datacollection.adaptor.FileAdaptor";
    String recordType = "MyRecordType_" + ts;
    
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      
      File in1Dir = new File(dataDir + "/input");
      in1Dir.mkdirs();
      int lineCount = 118;
      File inputFile = makeTestFile(dataDir + "/input/in2.txt",lineCount);
      long size = inputFile.length();
      
      String logFile = inputFile.getAbsolutePath();
      System.out.println("Output:" + logFile);
      System.out.println("File:" + inputFile.length());
      BackfillingLoader loader = new BackfillingLoader(conf,cluster,machine,adaptorName,recordType,logFile);
      loader.process();
      
      File finalOutputFile = new File(dataDir + "/input/in2.txt.sav");
      
      Assert.assertTrue("Input file exists", inputFile.exists() == false);
      Assert.assertTrue("Final input file exists", finalOutputFile.exists() == true);
      
      String doneFile = null;
      File directory = new File(dataDir  + "/log/");
      String[] files = directory.list();
      for(String file: files) {
        if ( file.endsWith(".done") ){
          doneFile = dataDir  + "/log/" + file;
          break;
        }
      }
      
     long seqId = validateDataSink(fs,conf,doneFile,finalOutputFile,
          cluster, recordType,  machine, logFile);
      Assert.assertTrue(seqId == size);
      
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
    try {
      FileUtils.deleteDirectory(new File(dataDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  
  
  public void testBackfillingLoaderWithCharFileTailingAdaptorUTF8NewLineEscapedBigFile() {
    String tmpDir = System.getProperty("test.build.data", "/tmp");
    long ts = System.currentTimeMillis();
    String dataDir = tmpDir + "/TestBackfillingLoader_" + ts;

    Configuration conf = new Configuration();
    conf.set("writer.hdfs.filesystem", "file:///");
    conf.set("chukwaCollector.outputDir", dataDir  + "/log/");
    conf.set("chukwaCollector.rotateInterval", "" + (Integer.MAX_VALUE -1));
    
    
    String machine = "machine_" + ts;
    String adaptorName = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped";
    String recordType = "MyRecordType_" + ts;
    
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      
      File in1Dir = new File(dataDir + "/input");
      in1Dir.mkdirs();
      int lineCount = 1024*1024;//34MB
      File inputFile = makeTestFile(dataDir + "/input/in1.txt",lineCount);
      long size = inputFile.length();
      
      String logFile = inputFile.getAbsolutePath();
      System.out.println("Output:" + logFile);
      System.out.println("File:" + inputFile.length());
      BackfillingLoader loader = new BackfillingLoader(conf,cluster,machine,adaptorName,recordType,logFile);
      loader.process();
      
      File finalOutputFile = new File(dataDir + "/input/in1.txt.sav");
      
      Assert.assertTrue(inputFile.exists() == false);
      Assert.assertTrue(finalOutputFile.exists() == true);
      
      String doneFile = null;
      File directory = new File(dataDir  + "/log/");
      String[] files = directory.list();
      for(String file: files) {
        if ( file.endsWith(".done") ){
          doneFile = dataDir  + "/log/" + file;
          break;
        }
      }
      
      long seqId = validateDataSink(fs,conf,doneFile,finalOutputFile,
          cluster, recordType,  machine, logFile);
     
      Assert.assertTrue(seqId == size);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
    try {
      FileUtils.deleteDirectory(new File(dataDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  
  public void testBackfillingLoaderWithCharFileTailingAdaptorUTF8NewLineEscapedBigFileLocalWriter() {
    String tmpDir = System.getProperty("test.build.data", "/tmp");
    long ts = System.currentTimeMillis();
    String dataDir = tmpDir + "/TestBackfillingLoader_" + ts;

    Configuration conf = new Configuration();
    conf.set("writer.hdfs.filesystem", "file:///");
    conf.set("chukwaCollector.outputDir", dataDir  + "/log/");
    conf.set("chukwaCollector.rotateInterval", "" + (Integer.MAX_VALUE -1));
    conf.set("chukwaCollector.localOutputDir", dataDir  + "/log/");
    conf.set("chukwaCollector.writerClass", "org.apache.hadoop.chukwa.datacollection.writer.localfs.LocalWriter");
    conf.set("chukwaCollector.minPercentFreeDisk", "2");//so unit tests pass on machines with full-ish disks

    String machine = "machine_" + ts;
    String adaptorName = "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped";
    String recordType = "MyRecordType_" + ts;
    
    try {
      FileSystem fs = FileSystem.getLocal(conf);
      
      File in1Dir = new File(dataDir + "/input");
      in1Dir.mkdirs();
      int lineCount = 1024*1024*2;//64MB
      File inputFile = makeTestFile(dataDir + "/input/in1.txt",lineCount);
      long size = inputFile.length();
      
      String logFile = inputFile.getAbsolutePath();
      System.out.println("Output:" + logFile);
      System.out.println("File:" + inputFile.length());
      BackfillingLoader loader = new BackfillingLoader(conf,cluster,machine,adaptorName,recordType,logFile);
      loader.process();
      
      File finalOutputFile = new File(dataDir + "/input/in1.txt.sav");
      
      Assert.assertTrue(inputFile.exists() == false);
      Assert.assertTrue(finalOutputFile.exists() == true);
      
      String doneFile = null;
      File directory = new File(dataDir  + "/log/");
      String[] files = directory.list();
      for(String file: files) {
        if ( file.endsWith(".done") ){
          doneFile = dataDir  + "/log/" + file;
          break;
        }
      }
      
      long seqId = validateDataSink(fs,conf,doneFile,finalOutputFile,
          cluster, recordType,  machine, logFile);
     
      Assert.assertTrue(seqId == size);
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail();
    }
    try {
      FileUtils.deleteDirectory(new File(dataDir));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  protected long validateDataSink(FileSystem fs,Configuration conf, String dataSinkFile, File logFile, 
      String cluster,String dataType, String source, String application) throws Throwable {
    AvroParquetReader<GenericRecord> reader = null;
    long lastSeqId = -1;
    FileOutputStream out = null;
    try {
      Schema chukwaAvroSchema = ChukwaAvroSchema.getSchema();
      AvroReadSupport.setRequestedProjection(conf, chukwaAvroSchema);
      reader = new AvroParquetReader<GenericRecord>(conf, new Path(dataSinkFile));

      String dataSinkDumpName = dataSinkFile + ".dump";
      out = new FileOutputStream(new File(dataSinkDumpName), true);

      GenericRecord record = null;
      while ( true ) {
        record = reader.read();
        if(record == null)
          break;
        Assert.assertTrue(record.get("tags").toString().contains(cluster));
        Assert.assertTrue(dataType.equals(record.get("dataType")));
        Assert.assertTrue(source.equals(record.get("source")));
        byte[] data = ((ByteBuffer)record.get("data")).array();
        out.write(data);
        lastSeqId = ((Long)record.get("seqId")).longValue();
      }
      
      out.close();
      out = null;
      reader.close();
      reader = null;
      
      String dataSinkMD5 = MD5.checksum(new File(dataSinkDumpName));
      String logFileMD5 = MD5.checksum(logFile);
      Assert.assertTrue(dataSinkMD5.equals(logFileMD5));
    }
    finally {
      if (out != null) {
        out.close();
      }
      
      if (reader != null) {
        reader.close();
      }
    }
   
    
    return lastSeqId;
  }
  
  private File makeTestFile(final String name, int size) throws IOException {
    File tmpOutput = new File(name);
    
    FileOutputStream fos = new FileOutputStream(tmpOutput);

    PrintWriter pw = new PrintWriter(fos);
    for (int i = 0; i < size; ++i) {
      pw.print(i + " ");
      pw.println("abcdefghijklmnopqrstuvwxyz");
    }
    pw.flush();
    pw.close();
    fos.close();
    return tmpOutput;
  }
  
}
