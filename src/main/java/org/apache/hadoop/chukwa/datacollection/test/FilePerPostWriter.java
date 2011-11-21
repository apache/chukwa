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
package org.apache.hadoop.chukwa.datacollection.test;

import java.io.IOException;
import java.net.URI;


import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.*;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter.CommitStatus;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter.StatReportingTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.mortbay.log.Log;

/**
 * A writer that writes a file for each post. Intended ONLY for architectural
 * performance comparisons.  Do not use this in production.
 *
 */
public class FilePerPostWriter extends SeqFileWriter {

  String baseName;
  AtomicLong counter = new AtomicLong(0);
  
  protected FileSystem fs = null;
  protected Configuration conf = null;

  protected String outputDir = null;
//  private Calendar calendar = Calendar.getInstance();

  protected Path currentPath = null;
  protected String currentFileName = null;

  
  @Override
  public synchronized CommitStatus add(List<Chunk> chunks) throws WriterException {

    try {
      String newName = baseName +"_" +counter.incrementAndGet();
      Path newOutputPath = new Path(newName + ".done");
      FSDataOutputStream currentOutputStr = fs.create(newOutputPath);
      currentPath = newOutputPath;
      currentFileName = newName;
      // Uncompressed for now
      SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, currentOutputStr,
          ChukwaArchiveKey.class, ChunkImpl.class,
          SequenceFile.CompressionType.NONE, null);
    
      ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();
      
      if (System.currentTimeMillis() >= nextTimePeriodComputation) {
        computeTimePeriod();
      }

      for (Chunk chunk : chunks) {
        archiveKey.setTimePartition(timePeriod);
        archiveKey.setDataType(chunk.getDataType());
        archiveKey.setStreamName(chunk.getTags() + "/" + chunk.getSource()
            + "/" + chunk.getStreamName());
        archiveKey.setSeqId(chunk.getSeqID());

        if (chunk != null) {
          // compute size for stats
          dataSize += chunk.getData().length;
          bytesThisRotate += chunk.getData().length;
          seqFileWriter.append(archiveKey, chunk);
        }

      }
      
      seqFileWriter.close();
      currentOutputStr.close();
    } catch(IOException e) {
      throw new WriterException(e);
    }
    return COMMIT_OK;
  }

  @Override
  public void close() {
  }

  @Override
  public void init(Configuration conf) throws WriterException {
    try {
      this.conf = conf;
      outputDir = conf.get(SeqFileWriter.OUTPUT_DIR_OPT, "/chukwa");
      baseName = outputDir + "/"+System.currentTimeMillis()+ "_" + localHostAddr.hashCode();
      
      String fsname = conf.get("writer.hdfs.filesystem");
      if (fsname == null || fsname.equals("")) {
        // otherwise try to get the filesystem from hadoop
        fsname = conf.get("fs.default.name");
      }

      fs = FileSystem.get(new URI(fsname), conf);
      isRunning = true;
      
      statTimer = new Timer();
      statTimer.schedule(new StatReportingTask(), 1000,
          STAT_INTERVAL_SECONDS * 1000);


      nextTimePeriodComputation = 0;
    } catch(Exception e) {
      throw new WriterException(e);
    }
      
  }

}
