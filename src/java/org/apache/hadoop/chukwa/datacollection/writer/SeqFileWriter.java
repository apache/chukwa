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


import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.io.IOException;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

/**
 * This class <b>is</b> thread-safe -- rotate() and save() both synchronize on
 * this object.
 * 
 */
public class SeqFileWriter extends PipelineableWriter implements ChukwaWriter {
  static Logger log = Logger.getLogger(SeqFileWriter.class);
  public static boolean ENABLE_ROTATION_ON_CLOSE = true;

  protected int STAT_INTERVAL_SECONDS = 30;
  private int rotateInterval = 1000 * 60 * 5;
  private int offsetInterval = 1000 * 30;
  private boolean if_fixed_interval = false;
  static final int ACQ_WAIT_ON_TERM = 500; //ms to wait for lock on a SIGTERM before aborting
  
  public static final String STAT_PERIOD_OPT = "chukwaCollector.stats.period";
  public static final String ROTATE_INTERVAL_OPT = "chukwaCollector.rotateInterval";
  public static final String IF_FIXED_INTERVAL_OPT = "chukwaCollector.isFixedTimeRotatorScheme";
  public static final String FIXED_INTERVAL_OFFSET_OPT = "chukwaCollector.fixedTimeIntervalOffset";
  public static final String OUTPUT_DIR_OPT= "chukwaCollector.outputDir";
  protected static String localHostAddr = null;
  
  protected final Semaphore lock = new Semaphore(1, true);
  
  protected FileSystem fs = null;
  protected Configuration conf = null;

  protected String outputDir = null;
  private Calendar calendar = Calendar.getInstance();

  protected Path currentPath = null;
  protected String currentFileName = null;
  protected FSDataOutputStream currentOutputStr = null;
  protected SequenceFile.Writer seqFileWriter = null;

  protected long timePeriod = -1;
  protected long nextTimePeriodComputation = -1;
  
  protected Timer rotateTimer = null;  
  protected Timer statTimer = null;
  
  protected volatile long dataSize = 0;
  protected volatile long bytesThisRotate = 0;
  protected volatile boolean isRunning = false;

  static {
    try {
      localHostAddr = "_" + InetAddress.getLocalHost().getHostName() + "_";
    } catch (UnknownHostException e) {
      localHostAddr = "-NA-";
    }
  }
  
  public SeqFileWriter() {}
  
  public long getBytesWritten() {
    return dataSize;
  }
  
  public void init(Configuration conf) throws WriterException {
    outputDir = conf.get(OUTPUT_DIR_OPT, "/chukwa");

    this.conf = conf;

    rotateInterval = conf.getInt(ROTATE_INTERVAL_OPT,rotateInterval);
    if_fixed_interval = conf.getBoolean(IF_FIXED_INTERVAL_OPT,if_fixed_interval);
    offsetInterval = conf.getInt(FIXED_INTERVAL_OFFSET_OPT,offsetInterval);
    
    STAT_INTERVAL_SECONDS = conf.getInt(STAT_PERIOD_OPT, STAT_INTERVAL_SECONDS);

    // check if they've told us the file system to use
    String fsname = conf.get("writer.hdfs.filesystem");
    if (fsname == null || fsname.equals("")) {
      // otherwise try to get the filesystem from hadoop
      fsname = conf.get("fs.default.name");
    }

    log.info("rotateInterval is " + rotateInterval);
    if(if_fixed_interval)
      log.info("using fixed time interval scheme, " +
              "offsetInterval is " + offsetInterval);
    else
      log.info("not using fixed time interval scheme");
    log.info("outputDir is " + outputDir);
    log.info("fsname is " + fsname);
    log.info("filesystem type from core-default.xml is "
        + conf.get("fs.hdfs.impl"));

    if (fsname == null) {
      log.error("no filesystem name");
      throw new WriterException("no filesystem");
    }
    try {
      fs = FileSystem.get(new URI(fsname), conf);
      if (fs == null) {
        log.error("can't connect to HDFS at " + fs.getUri() + " bail out!");
        DaemonWatcher.bailout(-1);
      }
    } catch (Throwable e) {
      log.error(
          "can't connect to HDFS, trying default file system instead (likely to be local)",
          e);
      DaemonWatcher.bailout(-1);
    }

    // Setup everything by rotating

    isRunning = true;
    rotate();

    statTimer = new Timer();
    statTimer.schedule(new StatReportingTask(), 1000,
        STAT_INTERVAL_SECONDS * 1000);

  }

  public class StatReportingTask extends TimerTask {
    private long lastTs = System.currentTimeMillis();

    public void run() {

      long time = System.currentTimeMillis();
      long currentDs = dataSize;
      dataSize = 0;

      long interval = time - lastTs;
      lastTs = time;

      long dataRate = 1000 * currentDs / interval; // kb/sec
      log.info("stat:datacollection.writer.hdfs dataSize=" + currentDs
          + " dataRate=" + dataRate);
    }
    
    public StatReportingTask() {}
  };

  void rotate() {
     if (rotateTimer != null) {
      rotateTimer.cancel();
    } 
     
    if(!isRunning)
      return;
    
    calendar.setTimeInMillis(System.currentTimeMillis());

    String newName = new java.text.SimpleDateFormat("yyyyMMddHHmmssSSS")
        .format(calendar.getTime());
    newName += localHostAddr + new java.rmi.server.UID().toString();
    newName = newName.replace("-", "");
    newName = newName.replace(":", "");
    newName = newName.replace(".", "");
    newName = outputDir + "/" + newName.trim();

    try {
      lock.acquire();

      FSDataOutputStream previousOutputStr = currentOutputStr;
      Path previousPath = currentPath;
      String previousFileName = currentFileName;

      if (previousOutputStr != null) {
        boolean closed = false;
        try {
          log.info("closing sink file" + previousFileName);
          previousOutputStr.close();
          closed = true;
        }catch (Throwable e) {
          log.error("couldn't close file" + previousFileName, e);
          //we probably have an orphaned 0 byte file at this point due to an
          //intermitant HDFS outage. Once HDFS comes up again we'll be able to
          //close it, although it will be empty.
        }

        if (bytesThisRotate > 0) {
          if (closed) {
            log.info("rotating sink file " + previousPath);
            fs.rename(previousPath, new Path(previousFileName + ".done"));
          }
          else {
            log.warn(bytesThisRotate + " bytes potentially lost, since " +
                    previousPath + " could not be closed.");
          }
        } else {
          log.info("no chunks written to " + previousPath + ", deleting");
          fs.delete(previousPath, false);
        }
      }

      Path newOutputPath = new Path(newName + ".chukwa");
      FSDataOutputStream newOutputStr = fs.create(newOutputPath);
      // Uncompressed for now
      seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
          ChukwaArchiveKey.class, ChunkImpl.class,
          SequenceFile.CompressionType.NONE, null);

      // reset these once we know that seqFileWriter was created
      currentOutputStr = newOutputStr;
      currentPath = newOutputPath;
      currentFileName = newName;
      bytesThisRotate = 0;
    } catch (Throwable e) {
      log.warn("Got an exception trying to rotate. Will try again in " +
              rotateInterval/1000 + " seconds." ,e);
    } finally {
      lock.release();
    }
    
    // Schedule the next timer
    scheduleNextRotation();

  }

  /**
   * Schedules the rotate task using either a fixed time interval scheme or a
   * relative time interval scheme as specified by the
   * chukwaCollector.isFixedTimeRotatorScheme configuration parameter. If the
   * value of this parameter is true then next rotation will be scheduled at a
   * fixed offset from the current rotateInterval. This fixed offset is
   * provided by chukwaCollector.fixedTimeIntervalOffset configuration
   * parameter.
   */
  void scheduleNextRotation(){
    long delay = rotateInterval;
    if (if_fixed_interval) {
      long currentTime = System.currentTimeMillis();
      delay = getDelayForFixedInterval(currentTime, rotateInterval, offsetInterval);
    }
    rotateTimer = new Timer();
    rotateTimer.schedule(new TimerTask() {
      public void run() {
        rotate();
      }
    }, delay);
  }

  /**
   * Calculates delay for scheduling the next rotation in case of
   * FixedTimeRotatorScheme. This delay is the time difference between the
   * currentTimestamp (t1) and the next time the collector should rotate the
   * sequence files (t2). t2 is the time when the current rotateInterval ends
   * plus an offset (as set by chukwaCollector.FixedTimeIntervalOffset).
   * So, delay = t2 - t1
   *
   * @param currentTime - the current timestamp
   * @param rotateInterval - chukwaCollector.rotateInterval
   * @param offsetInterval - chukwaCollector.fixedTimeIntervalOffset
   * @return delay for scheduling next rotation
   */
  long getDelayForFixedInterval(long currentTime, long rotateInterval, long offsetInterval){
    // time since last rounded interval
    long remainder = (currentTime % rotateInterval);
    long prevRoundedInterval = currentTime - remainder;
    long nextRoundedInterval = prevRoundedInterval + rotateInterval;
    long delay = nextRoundedInterval - currentTime + offsetInterval;

    if (log.isInfoEnabled()) {
     log.info("currentTime="+currentTime+" prevRoundedInterval="+
             prevRoundedInterval+" nextRoundedInterval" +
            "="+nextRoundedInterval+" delay="+delay);
    }

    return delay;
  }


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
  
  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    COMMIT_PENDING result = new COMMIT_PENDING(chunks.size());
    if (!isRunning) {
      log.info("Collector not ready");
      throw new WriterException("Collector not ready");
    }

    if (chunks != null) {
      ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();
      
      if (System.currentTimeMillis() >= nextTimePeriodComputation) {
        computeTimePeriod();
      }
      try {
        lock.acquire();
        for (Chunk chunk : chunks) {
          archiveKey.setTimePartition(timePeriod);
          archiveKey.setDataType(chunk.getDataType());
          archiveKey.setStreamName(chunk.getTags() + "/" + chunk.getSource()
              + "/" + chunk.getStreamName());
          archiveKey.setSeqId(chunk.getSeqID());

          if (chunk != null) {
            seqFileWriter.append(archiveKey, chunk);

            // compute size for stats only if append succeeded. Note though that
            // seqFileWriter.append can continue taking data for quite some time
            // after HDFS goes down while the client is trying to reconnect. Hence
            // these stats might not reflect reality during an HDFS outage.
            dataSize += chunk.getData().length;
            bytesThisRotate += chunk.getData().length;

            String futureName = currentPath.getName().replace(".chukwa", ".done");
            result.addPend(futureName, currentOutputStr.getPos());
          }

        }
      }
      catch (IOException e) {
        log.error("IOException when trying to write a chunk, Collector will return error and keep running.", e);
        return COMMIT_FAIL;
      }
      catch (Throwable e) {
        // We don't want to loose anything
        log.fatal("IOException when trying to write a chunk, Collector is going to exit!", e);
        DaemonWatcher.bailout(-1);
        isRunning = false;
      } finally {
        lock.release();
      }
    }
    return result;
  }

  public void close() {
    
    isRunning = false;

    if (statTimer != null) {
      statTimer.cancel();
    }

    if (rotateTimer != null) {
      rotateTimer.cancel();
    }

    // If we are here it's either because of an HDFS exception
    // or Collector has received a kill -TERM
    boolean gotLock = false;
    try {
      gotLock = lock.tryAcquire(ACQ_WAIT_ON_TERM, TimeUnit.MILLISECONDS);
      if(gotLock) {
        
        if (this.currentOutputStr != null) {
          this.currentOutputStr.close();
        }
        if(ENABLE_ROTATION_ON_CLOSE)
          if(bytesThisRotate > 0)
            fs.rename(currentPath, new Path(currentFileName + ".done"));
          else
            fs.delete(currentPath, false);
      }
    } catch (Throwable e) {
     log.warn("cannot rename dataSink file:" + currentPath,e);
    } finally {
      if(gotLock)
        lock.release();
    }
  }

}
