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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


class FileAdaptorTailer extends Thread {
  static Logger log = Logger.getLogger(FileAdaptorTailer.class);
  private List<FileAdaptor> adaptors = null;
  private static Configuration conf = null;
  private Object lock = new Object();
  
  /**
   * How often to call each adaptor.
   */
  int DEFAULT_SAMPLE_PERIOD_MS = 1000 * 10;
  int SAMPLE_PERIOD_MS = DEFAULT_SAMPLE_PERIOD_MS;

  
  public FileAdaptorTailer() {
    
    if (conf == null) {
      ChukwaAgent agent = ChukwaAgent.getAgent();
      if (agent != null) {
        conf = agent.getConfiguration();
        if (conf != null) {
          SAMPLE_PERIOD_MS = conf.getInt(
              "chukwaAgent.adaptor.context.switch.time",
              DEFAULT_SAMPLE_PERIOD_MS);
        }
      }
    }
    
    // iterations are much more common than adding a new adaptor
    adaptors = new CopyOnWriteArrayList<FileAdaptor>();

    setDaemon(true);
    start();// start the FileAdaptorTailer thread
  }
  @Override
  public void run() {
    while(true) {
      try {

        while (adaptors.size() == 0) {
          synchronized (lock) {
            try {
              log.info("Waiting queue is empty");
              lock.wait();
            } catch (InterruptedException e) {
              // do nothing
            }
          }
        }
        
        long startTime = System.currentTimeMillis();
        for (FileAdaptor adaptor: adaptors) {
          log.info("calling sendFile for " + adaptor.toWatch.getCanonicalPath());
          adaptor.sendFile(); 
        }
        
        long timeToReadFiles = System.currentTimeMillis() - startTime;
        if (timeToReadFiles < SAMPLE_PERIOD_MS) {
          Thread.sleep(SAMPLE_PERIOD_MS);
        }
        
      }catch (Throwable e) {
        log.warn("Exception in FileAdaptorTailer:",e);
      }
    }
  }
  
  public void addFileAdaptor(FileAdaptor adaptor) {
    adaptors.add(adaptor);
    synchronized (lock) {
      lock.notifyAll();
    }
  }
  
  public void removeFileAdaptor(FileAdaptor adaptor) {
    adaptors.remove(adaptor);
  }
}

/**
 * File Adaptor push small size file in one chunk to collector
 */
public class FileAdaptor extends AbstractAdaptor {

  static Logger log = Logger.getLogger(FileAdaptor.class);
  static FileAdaptorTailer tailer = null;
  
  static final int DEFAULT_TIMEOUT_PERIOD = 5*60*1000;
  static int TIMEOUT_PERIOD = DEFAULT_TIMEOUT_PERIOD;
  
  static {
    tailer = new FileAdaptorTailer();
  }
  
  private long startTime = 0;
  private long timeOut = 0;
  
  protected volatile boolean finished = false;
  File toWatch;
  protected RandomAccessFile reader = null;
  protected long fileReadOffset;
  protected boolean deleteFileOnClose = false;
  protected boolean shutdownCalled = false;
  
  /**
   * The logical offset of the first byte of the file
   */
  private long offsetOfFirstByte = 0;

  public void start(long bytes) {
    // in this case params = filename
    log.info("adaptor id: " + adaptorID + " started file adaptor on file "
        + toWatch);
    this.startTime = System.currentTimeMillis();
    TIMEOUT_PERIOD = control.getConfiguration().getInt(
        "chukwaAgent.adaptor.fileadaptor.timeoutperiod",
        DEFAULT_TIMEOUT_PERIOD);
    this.timeOut = startTime + TIMEOUT_PERIOD;
    
    tailer.addFileAdaptor(this);
  }

  void sendFile() {
    long now = System.currentTimeMillis() ;
    long oneMinAgo = now - (60*1000);
    if (toWatch.exists()) {
     if (toWatch.lastModified() > oneMinAgo && now < timeOut) {
       log.info("Last modified time less than one minute, keep waiting");
       return;
     } else {
       try {
         
         long bufSize = toWatch.length();
         byte[] buf = new byte[(int) bufSize];
         
         reader = new RandomAccessFile(toWatch, "r");
         reader.read(buf);
         reader.close();
         reader = null;
         
         long fileTime = toWatch.lastModified();
         int bytesUsed = extractRecords(dest, 0, buf, fileTime);
         this.fileReadOffset = bytesUsed;
         finished = true;
         deregisterAndStop();
         cleanUp();
       } catch(Exception e) {
         log.warn("Exception while trying to read: " + toWatch.getAbsolutePath(),e);
       }  finally {
         if (reader != null) {
           try {
             reader.close();
           } catch (Exception e) {
            // do nothing
          }
           reader = null;
         }
       }
     }
    } else {
      if (now > timeOut) {
        finished = true;
        log.warn("Couldn't read this file: " + toWatch.getAbsolutePath());
        deregisterAndStop();
        cleanUp() ;
      }
    }
  }
  
  private void cleanUp() {
    tailer.removeFileAdaptor(this);
    if (reader != null) {
      try {
        reader.close();
      } catch (Exception e) {
       // do nothing
     }
      reader = null;
    } 
  }
  

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy) {
    log.info("Enter Shutdown:" + shutdownPolicy.name()+ " - ObjectId:" + this);
    switch(shutdownPolicy) {
      case GRACEFULLY : {
        int retry = 0;
        while (!finished && retry < 60) {
          try {
            log.info("GRACEFULLY Retry:" + retry);
            Thread.sleep(1000);
            retry++;
          } catch (InterruptedException ex) {
          }
        } 
      }
      break;
      case WAIT_TILL_FINISHED : {
        int retry = 0;
        while (!finished) {
          try {
            if (retry%100 == 0) {
              log.info("WAIT_TILL_FINISHED Retry:" + retry);
            }

            Thread.sleep(1000);
            retry++;
          } catch (InterruptedException ex) {
          }
        } 
      }

      break;
      default :
        cleanUp();
        break;
    }

    if (deleteFileOnClose && toWatch != null) {
      if (log.isDebugEnabled()) {
        log.debug("About to delete " + toWatch.getAbsolutePath());
      }
      if (toWatch.delete()) {
        if (log.isInfoEnabled()) {
          log.debug("Successfully deleted " + toWatch.getAbsolutePath());
        }
      } else {
        if (log.isEnabledFor(Level.WARN)) {
          log.warn("Could not delete " + toWatch.getAbsolutePath() + " (for unknown reason)");
        }
      }
    }
    
    log.info("Exist Shutdown:" + shutdownPolicy.name()+ " - ObjectId:" + this);
    return fileReadOffset + offsetOfFirstByte;
  }
  
  public String parseArgs(String params) {

    String[] words = params.split(" ");
    if (words.length == 2) {
      if (words[1].equals("delete")) {
        deleteFileOnClose = true;
        toWatch = new File(words[0]);
      } else {
        offsetOfFirstByte = Long.parseLong(words[0]);
        toWatch = new File(words[1]);
      }
    } else if (words.length == 3) {
      offsetOfFirstByte = Long.parseLong(words[0]);
      toWatch = new File(words[1]);
      deleteFileOnClose = words[2].equals("delete");
    } else {
      toWatch = new File(params);
    }
    return toWatch.getAbsolutePath();
  }

  /**
   * Extract records from a byte sequence
   * 
   * @param eq
   *          the queue to stick the new chunk[s] in
   * @param buffOffsetInFile
   *          the byte offset in the stream at which buf[] begins
   * @param buf
   *          the byte buffer to extract records from
   * @return the number of bytes processed
   * @throws InterruptedException
   */
  protected int extractRecords(final ChunkReceiver eq, long buffOffsetInFile,
      byte[] buf, long fileTime) throws InterruptedException {
    final ChunkImpl chunk = new ChunkImpl(type, toWatch.getAbsolutePath(),
        buffOffsetInFile + buf.length, buf, this);
    chunk.addTag("time=\"" + fileTime + "\"");
    log.info("Adding " + toWatch.getAbsolutePath() + " to the queue");
    eq.add(chunk);
    log.info( toWatch.getAbsolutePath() + " added to the queue");
    return buf.length;
  }

  @Override
  public String getCurrentStatus() {
    return type.trim() + " " + offsetOfFirstByte + " " + toWatch.getPath();
  }

}
