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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkBuilder;
import org.apache.hadoop.chukwa.datacollection.writer.parquet.ChukwaParquetWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Assert;

import junit.framework.TestCase;

public class TestChukwaParquetWriter extends TestCase {
  private final static Logger LOG = Logger.getLogger(TestChukwaParquetWriter.class);
  /**
   * Test records are written properly.
   */
  public void testWrite() {
    // Write 10 chunks
    ArrayList<Chunk> chunks = new ArrayList<Chunk>();
    for(int i=0;i<10;i++) {
      ChunkBuilder c = new ChunkBuilder();
      c.addRecord(ByteBuffer.allocate(Integer.SIZE).putInt(i).array());
      chunks.add(c.getChunk());
    }
    try {
      Configuration conf = new Configuration();
      String outputPath = System.getProperty("test.log.dir")+"/testParquet";
      conf.set("chukwaCollector.outputDir", outputPath);
      ChukwaWriter parquetWriter = new ChukwaParquetWriter(conf);
      parquetWriter.add(chunks);
      parquetWriter.close();
      FileSystem fs = FileSystem.get(conf);
      // Verify 10 chunks are written
      Path file = new Path(outputPath);
      FileStatus[] status = fs.listStatus(file);
      for(FileStatus finfo : status) {
        if(finfo.getPath().getName().contains(".done")) {
          LOG.info("File name: "+finfo.getPath().getName());
          LOG.info("File Size: " + finfo.getLen());
          ParquetReader<GenericRecord> pr = ParquetReader.builder(new AvroReadSupport<GenericRecord>(), finfo.getPath()).build();
          for(int i=0; i< 10; i++) {
            GenericRecord nextRecord = pr.read();
            int expected = ByteBuffer.wrap(chunks.get(i).getData()).getInt();
            LOG.info("expected: " + expected);
            ByteBuffer content = (ByteBuffer) nextRecord.get("data");
            int actual = content.getInt();
            LOG.info("actual: " + actual);
            Assert.assertSame(expected, actual);
          }
        }
        fs.delete(finfo.getPath(), true);
      }
    } catch (WriterException e) {
      Assert.fail(e.getMessage());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  /**
   * Test file rotation interval.
   */
  public void testRotate() {
    // Write 10 chunks
    ArrayList<Chunk> chunks = new ArrayList<Chunk>();
    for(int i=0;i<10;i++) {
      ChunkBuilder c = new ChunkBuilder();
      c.addRecord(ByteBuffer.allocate(Integer.SIZE).putInt(i).array());
      chunks.add(c.getChunk());
    }
    try {
      Configuration conf = new Configuration();
      String outputPath = System.getProperty("test.log.dir")+"/testParquetRotate";
      conf.set("chukwaCollector.outputDir", outputPath);
      conf.setLong("chukwaCollector.rotateInterval", 3000L);
      ChukwaWriter parquetWriter = new ChukwaParquetWriter(conf);
      for(int i=0; i<2; i++) {
        parquetWriter.add(chunks);
        try {
          Thread.sleep(3000L);
        } catch (InterruptedException e) {
          Assert.fail(e.getMessage());
        }
      }
      parquetWriter.close();
      FileSystem fs = FileSystem.get(conf);
      // Verify 10 chunks are written
      Path file = new Path(outputPath);
      FileStatus[] status = fs.listStatus(file);
      Assert.assertTrue(status.length >= 2);
    } catch (WriterException e) {
      Assert.fail(e.getMessage());
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

}
