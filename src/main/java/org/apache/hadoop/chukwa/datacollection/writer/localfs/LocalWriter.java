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

package org.apache.hadoop.chukwa.datacollection.writer.localfs;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

/**
 * <p>This class <b>is</b> thread-safe -- rotate() and save() both synchronize on
 * lock object.
 * </p>
 * <p>
 * Write data to a local fileSystem then move it to the remote HDFS
 * <br>
 * Warning:
 * <br>
 * There's no lock/waiting time for the remote client.
 * The connection is released as soon as the last append is done,
 * so therefore there is no guarantee that this class will not loose 
 * any data.
 * <br>
 * This class has been designed this way for performance reason.
 * </p>
 * <p>
 * In order to use this class, you need to define some parameters,
 * in chukwa-collector-conf.xml
 * <p>
 * <br>
 *  &lt;property&gt;<br>
 *   &lt;name&gt;chukwaCollector.localOutputDir&lt;/name&gt;<br>
 *   &lt;value&gt;/grid/0/gs/chukwa/chukwa-0.1.2/dataSink/&lt;/value&gt;<br>
 *   &lt;description&gt;Chukwa data sink directory&lt;/description&gt;<br>
 *  &lt;/property&gt;<br>
 *<br>
 *  &lt;property&gt;<br>
 *    &lt;name&gt;chukwaCollector.writerClass&lt;/name&gt;<br>
 *    &lt;value&gt;org.apache.hadoop.chukwa.datacollection.writer.localfs.LocalWriter&lt;/value&gt;<br>
 *    &lt;description&gt;Local chukwa writer&lt;/description&gt;<br>
 *  &lt;/property&gt;<br>
 * <br>
 */
public class LocalWriter implements ChukwaWriter {

  static Logger log = Logger.getLogger(LocalWriter.class);
  static final int STAT_INTERVAL_SECONDS = 30;
  static String localHostAddr = null;

  private final Object lock = new Object();
  private BlockingQueue<String> fileQueue = null;
  @SuppressWarnings("unused")
  private LocalToRemoteHdfsMover localToRemoteHdfsMover = null;
  private FileSystem fs = null;
  private Configuration conf = null;

  private String localOutputDir = null;
  private Calendar calendar = Calendar.getInstance();

  private Path currentPath = null;
  private String currentFileName = null;
  private FSDataOutputStream currentOutputStr = null;
  private SequenceFile.Writer seqFileWriter = null;
  private int rotateInterval = 1000 * 60;

 
  private volatile long dataSize = 0;
  private volatile boolean isRunning = false;
  
  private Timer rotateTimer = null;
  private Timer statTimer = null;
  
  
  private int initWriteChunkRetries = 10;
  private int writeChunkRetries = initWriteChunkRetries;
  private boolean chunksWrittenThisRotate = false;

  private long timePeriod = -1;
  private long nextTimePeriodComputation = -1;
  private int minPercentFreeDisk = 20;
  
  static {
    try {
      localHostAddr = "_" + InetAddress.getLocalHost().getHostName() + "_";
    } catch (UnknownHostException e) {
      localHostAddr = "-NA-";
    }
  }

  public void init(Configuration conf) throws WriterException {
    this.conf = conf;

    try {
      fs = FileSystem.getLocal(conf);
      localOutputDir = conf.get("chukwaCollector.localOutputDir",
          "/chukwa/datasink/");
      if (!localOutputDir.endsWith("/")) {
        localOutputDir += "/";
      }
      Path pLocalOutputDir = new Path(localOutputDir);
      if (!fs.exists(pLocalOutputDir)) {
        boolean exist = fs.mkdirs(pLocalOutputDir);
        if (!exist) {
          throw new WriterException("Cannot create local dataSink dir: "
              + localOutputDir);
        }
      } else {
        FileStatus fsLocalOutputDir = fs.getFileStatus(pLocalOutputDir);
        if (!fsLocalOutputDir.isDir()) {
          throw new WriterException("local dataSink dir is not a directory: "
              + localOutputDir);
        }
      }
    } catch (Throwable e) {
      log.fatal("Cannot initialize LocalWriter", e);
      DaemonWatcher.bailout(-1);
    }

    
    minPercentFreeDisk = conf.getInt("chukwaCollector.minPercentFreeDisk",20);
    
    rotateInterval = conf.getInt("chukwaCollector.rotateInterval",
        1000 * 60 * 5);// defaults to 5 minutes
   
    initWriteChunkRetries = conf
        .getInt("chukwaCollector.writeChunkRetries", 10);
    writeChunkRetries = initWriteChunkRetries;

    log.info("rotateInterval is " + rotateInterval);
    log.info("outputDir is " + localOutputDir);
    log.info("localFileSystem is " + fs.getUri().toString());
    log.info("minPercentFreeDisk is " + minPercentFreeDisk);
    
    // Setup everything by rotating
    rotate();

    rotateTimer = new Timer();
    rotateTimer.schedule(new RotateTask(), rotateInterval,
        rotateInterval);
    
    statTimer = new Timer();
    statTimer.schedule(new StatReportingTask(), 1000,
        STAT_INTERVAL_SECONDS * 1000);

    fileQueue = new LinkedBlockingQueue<String>();
    localToRemoteHdfsMover = new LocalToRemoteHdfsMover(fileQueue, conf);
    
  }

  private class RotateTask extends TimerTask {
        public void run() {
          rotate();
      };
  }
  
  private class StatReportingTask extends TimerTask {
    private long lastTs = System.currentTimeMillis();

    public void run() {

      long time = System.currentTimeMillis();
      long currentDs = dataSize;
      dataSize = 0;

      long interval = time - lastTs;
      lastTs = time;

      long dataRate = 1000 * currentDs / interval; // kb/sec
      log.info("stat:datacollection.writer.local.LocalWriter dataSize="
          + currentDs + " dataRate=" + dataRate);
    }
  };

  protected void computeTimePeriod() {
    synchronized (calendar) {
      calendar.setTimeInMillis(System.currentTimeMillis());
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      timePeriod = calendar.getTimeInMillis();
      calendar.add(Calendar.HOUR, 1);
      nextTimePeriodComputation = calendar.getTimeInMillis();
    }
  }


  /**
   *  Best effort, there's no guarantee that chunks 
   *  have really been written to disk
   */
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    if (!isRunning) {
      throw new WriterException("Writer not yet ready");
    }
    long now = System.currentTimeMillis();
    if (chunks != null) {
      try {
        chunksWrittenThisRotate = true;
        ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();

        synchronized (lock) {
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
              seqFileWriter.append(archiveKey, chunk);
              // compute size for stats
              dataSize += chunk.getData().length;
            }
          }
        }// End synchro
        long end = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
          log.debug("duration=" + (end-now) + " size=" + chunks.size());
        }
        
      } catch (IOException e) {
        writeChunkRetries--;
        log.error("Could not save the chunk. ", e);

        if (writeChunkRetries < 0) {
          log
              .fatal("Too many IOException when trying to write a chunk, Collector is going to exit!");
          DaemonWatcher.bailout(-1);
        }
        throw new WriterException(e);
      }
    }
    return COMMIT_OK;
  }

  protected void rotate() {
    isRunning = true;
    calendar.setTimeInMillis(System.currentTimeMillis());
    log.info("start Date [" + calendar.getTime() + "]");
    log.info("Rotate from " + Thread.currentThread().getName());

    String newName = new java.text.SimpleDateFormat("yyyyddHHmmssSSS")
        .format(calendar.getTime());
    newName += localHostAddr + new java.rmi.server.UID().toString();
    newName = newName.replace("-", "");
    newName = newName.replace(":", "");
    newName = newName.replace(".", "");
    newName = localOutputDir + "/" + newName.trim();

    synchronized (lock) {
      try {
        FSDataOutputStream previousOutputStr = currentOutputStr;
        Path previousPath = currentPath;
        String previousFileName = currentFileName;

        if (previousOutputStr != null) {
          previousOutputStr.close();
          if (chunksWrittenThisRotate) {
            fs.rename(previousPath, new Path(previousFileName + ".done"));
            fileQueue.add(previousFileName + ".done");
          } else {
            log.info("no chunks written to " + previousPath + ", deleting");
            fs.delete(previousPath, false);
          }
        }
        
        Path newOutputPath = new Path(newName + ".chukwa");
        FSDataOutputStream newOutputStr = fs.create(newOutputPath);
        
        currentOutputStr = newOutputStr;
        currentPath = newOutputPath;
        currentFileName = newName;
        chunksWrittenThisRotate = false;
        // Uncompressed for now
        seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
            ChukwaArchiveKey.class, ChunkImpl.class,
            SequenceFile.CompressionType.NONE, null);

      } catch (IOException e) {
        log.fatal("IO Exception in rotate. Exiting!", e);
        // Shutting down the collector
        // Watchdog will re-start it automatically
        DaemonWatcher.bailout(-1);
      }
    }
 
    // Check for disk space
    File directory4Space = new File(localOutputDir);
    long totalSpace = directory4Space.getTotalSpace();
    long freeSpace = directory4Space.getFreeSpace();
    long minFreeAvailable = (totalSpace * minPercentFreeDisk) /100;
    
    if (log.isDebugEnabled()) {
      log.debug("Directory: " + localOutputDir + ", totalSpace: " + totalSpace 
          + ", freeSpace: " + freeSpace + ", minFreeAvailable: " + minFreeAvailable
          + ", percentFreeDisk: " + minPercentFreeDisk);
    }
  
    if (freeSpace < minFreeAvailable) {
      log.fatal("No space left on device, Bail out!");
      DaemonWatcher.bailout(-1);
    } 
    
    log.debug("finished rotate()");
  }

  public void close() {
    synchronized (lock) {
  
      if (rotateTimer != null) {
        rotateTimer.cancel();
      }

      if (statTimer != null) {
        statTimer.cancel();
      }

      try {
        if (this.currentOutputStr != null) {
          this.currentOutputStr.close();

          if (seqFileWriter != null) {
            seqFileWriter.close();
          }
        }
        if (localToRemoteHdfsMover != null) {
          localToRemoteHdfsMover.shutdown();
        }
        
        fs.rename(currentPath, new Path(currentFileName + ".done"));
      } catch (IOException e) {
        log.error("failed to close and rename stream", e);
      }
    }
  }
}
