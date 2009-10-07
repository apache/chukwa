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
package org.apache.hadoop.chukwa.datacollection.writer;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkBuilder;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.localfs.LocalWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;




public class TestChukwaWriters extends TestCase{

  public void testWriters() {
    try {
      
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.getLocal(conf);

      
      ChukwaWriter seqWriter = new SeqFileWriter();
      ChukwaWriter localWriter = new LocalWriter();
      
      List<Chunk> chunksSeqWriter = new LinkedList<Chunk>();
      List<Chunk> chunksLocalWriter = new LinkedList<Chunk>();
      for(int i=0;i<10;i++) {
        ChunkBuilder cb1 = new ChunkBuilder();
        cb1.addRecord(("record-" +i) .getBytes());
        cb1.addRecord("foo" .getBytes());
        cb1.addRecord("bar".getBytes());
        cb1.addRecord("baz".getBytes());
        chunksSeqWriter.add(cb1.getChunk());
        
        ChunkBuilder cb2 = new ChunkBuilder();
        cb2.addRecord(("record-" +i) .getBytes());
        cb2.addRecord("foo" .getBytes());
        cb2.addRecord("bar".getBytes());
        cb2.addRecord("baz".getBytes());
        chunksLocalWriter.add(cb2.getChunk());
        
      }
      
      File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
      if (!tempDir.exists()) {
        tempDir.mkdirs();
      }
      
      String outputDirectory = tempDir.getPath() + "/testChukwaWriters_JB_" + System.currentTimeMillis() + "/";
      
      
      Configuration confSeqWriter = new Configuration();
      confSeqWriter.set("chukwaCollector.rotateInterval", "300000");
      confSeqWriter.set("writer.hdfs.filesystem", "file:///");
      String seqWriterOutputDir = outputDirectory +"/seqWriter/seqOutputDir";
      confSeqWriter.set(SeqFileWriter.OUTPUT_DIR_OPT, seqWriterOutputDir );
      
      seqWriter.init(confSeqWriter);
      Thread.sleep(5000);
      seqWriter.add(chunksSeqWriter);
      seqWriter.close();
      
      String seqWriterFile = null;
      
      File directory = new File(seqWriterOutputDir);
      String[] files = directory.list();
      for(String file: files) {
        if ( file.endsWith(".done") ){
          seqWriterFile = seqWriterOutputDir + File.separator + file;
          break;
        }
      }
      
      Assert.assertFalse(seqWriterFile == null);
      
      String seqWriterDump = dumpArchive(fs,conf,seqWriterFile);
      
      Configuration confLocalWriter = new Configuration();
      confSeqWriter.set("writer.hdfs.filesystem", "file:///");
      String localWriterOutputDir = outputDirectory +"/localWriter/localOutputDir";
      confLocalWriter.set("chukwaCollector.localOutputDir",localWriterOutputDir);
      confLocalWriter.set("chukwaCollector.rotateInterval", "300000");
      confLocalWriter.set("chukwaCollector.minPercentFreeDisk", "2");//so unit tests pass on 
      //machines with mostly-full disks

      
      String localWriterFile = null;
      localWriter.init(confLocalWriter);
      Thread.sleep(5000);
      localWriter.add(chunksLocalWriter);
      localWriter.close();

      directory = new File(localWriterOutputDir);
      files = directory.list();
      for(String file: files) {
        if ( file.endsWith(".done") ){
          localWriterFile = localWriterOutputDir + File.separator + file;
          break;
        }
      }
      
      Assert.assertFalse(localWriterFile == null);
      String localWriterDump = dumpArchive(fs,conf,localWriterFile);

      Assert.assertTrue(seqWriterDump.intern() == localWriterDump.intern());

      File fOutputDirectory = new File(outputDirectory);
      fOutputDirectory.delete();
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("Exception in TestChukwaWriters," + e.getMessage());
    }
    
  }
  
  protected String dumpArchive(FileSystem fs,Configuration conf, String file) throws Throwable {
    SequenceFile.Reader reader = null;
    try {
      reader = new SequenceFile.Reader(fs, new Path(file), conf);

      ChukwaArchiveKey key = new ChukwaArchiveKey();
      ChunkImpl chunk = ChunkImpl.getBlankChunk();

      StringBuilder sb = new StringBuilder();
      while (reader.next(key, chunk)) {
        sb.append("\nTimePartition: " + key.getTimePartition());
        sb.append("DataType: " + key.getDataType());
        sb.append("StreamName: " + key.getStreamName());
        sb.append("SeqId: " + key.getSeqId());
        sb.append("\t\t =============== ");

        sb.append("Cluster : " + chunk.getTags());
        sb.append("DataType : " + chunk.getDataType());
        sb.append("Source : " + chunk.getSource());
        sb.append("Application : " + chunk.getApplication());
        sb.append("SeqID : " + chunk.getSeqID());
        sb.append("Data : " + new String(chunk.getData()));
        return sb.toString();
      }
    } catch (Throwable e) {
     Assert.fail("Exception while reading SeqFile"+ e.getMessage());
     throw e;
    }
    
    finally {
      if (reader != null) {
        reader.close();
      }
    }
    return null;    
  }
}
