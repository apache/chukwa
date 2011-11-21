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

package org.apache.hadoop.chukwa.datacollection.adaptor.filetailer;


import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.adaptor.*;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * An adaptor that repeatedly tails a specified file, sending the new bytes.
 * This class does not split out records, but just sends everything up to end of
 * file. Subclasses can alter this behavior by overriding extractRecords().
 * 
 */
public class FileTailingAdaptor extends LWFTAdaptor {


  public static int MAX_RETRIES = 300;
  public static int GRACEFUL_PERIOD = 3 * 60 * 1000; // 3 minutes

  private int attempts = 0;
  private long gracefulPeriodExpired = 0l;
  private boolean adaptorInError = false;

  protected RandomAccessFile reader = null;

  public void start(long bytes) {
    super.start(bytes);
    log.info("chukwaAgent.fileTailingAdaptor.maxReadSize: " + MAX_READ_SIZE);
    this.attempts = 0;

    log.info("started file tailer " + adaptorID +  "  on file " + toWatch
        + " with first byte at offset " + offsetOfFirstByte);
  }
 
  
  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy) {
    
    log.info("Enter Shutdown:" + shutdownPolicy.name() + " - ObjectId:" + this);
    
    switch(shutdownPolicy) {
      case GRACEFULLY : 
      case WAIT_TILL_FINISHED :{
        if (toWatch.exists()) {
          int retry = 0;
          tailer.stopWatchingFile(this);
          TerminatorThread lastTail = new TerminatorThread(this);
          lastTail.setDaemon(true);
          lastTail.start();
          
          if (shutdownPolicy.ordinal() == AdaptorShutdownPolicy.GRACEFULLY.ordinal()) {
            while (lastTail.isAlive() && retry < 60) {
              try {
                log.info("GRACEFULLY Retry:" + retry);
                Thread.sleep(1000);
                retry++;
              } catch (InterruptedException ex) {
              }
            }
          } else {
            while (lastTail.isAlive()) {
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
        }
      }
      break;
      
      case HARD_STOP:
      default:
        tailer.stopWatchingFile(this);
        try {
          if (reader != null) {
            reader.close();
          }
          reader = null;
        } catch(Throwable e) {
         log.warn("Exception while closing reader:",e);
        }
        break;
    }
    log.info("Exit Shutdown:" + shutdownPolicy.name()+ " - ObjectId:" + this);
    return fileReadOffset + offsetOfFirstByte;
  }
  

  /**
   * Looks at the tail of the associated file, adds some of it to event queue
   * This method is not thread safe. Returns true if there's more data in the
   * file
   * 
   * @param eq the queue to write Chunks into
   */
  @Override
  public synchronized boolean tailFile()
      throws InterruptedException {
    boolean hasMoreData = false;

    try {
      if ((adaptorInError == true)
          && (System.currentTimeMillis() > gracefulPeriodExpired)) {
        if (!toWatch.exists()) {
          log.warn("Adaptor|" + adaptorID + "|attempts=" + attempts
              + "| File does not exist: " + toWatch.getAbsolutePath()
              + ", streaming policy expired.  File removed from streaming.");
        } else if (!toWatch.canRead()) {
          log.warn("Adaptor|" + adaptorID + "|attempts=" + attempts
              + "| File cannot be read: " + toWatch.getAbsolutePath()
              + ", streaming policy expired.  File removed from streaming.");
        } else {
          // Should have never been there
          adaptorInError = false;
          gracefulPeriodExpired = 0L;
          attempts = 0;
          return false;
        }

        deregisterAndStop();
        return false;
      } else if (!toWatch.exists() || !toWatch.canRead()) {
        if (adaptorInError == false) {
          long now = System.currentTimeMillis();
          gracefulPeriodExpired = now + GRACEFUL_PERIOD;
          adaptorInError = true;
          attempts = 0;
          log.warn("failed to stream data for: " + toWatch.getAbsolutePath()
              + ", graceful period will Expire at now:" + now + " + "
              + GRACEFUL_PERIOD + " secs, i.e:" + gracefulPeriodExpired);
        } else if (attempts % 10 == 0) {
          log.info("failed to stream data for: " + toWatch.getAbsolutePath()
              + ", attempt: " + attempts);
        }

        attempts++;
        return false; // no more data
      }

      if (reader == null) {
        reader = new RandomAccessFile(toWatch, "r");
        log.info("Adaptor|" + adaptorID
            + "|Opening the file for the first time|seek|" + fileReadOffset);
      }

      long len = 0L;
      try {
        RandomAccessFile newReader = new RandomAccessFile(toWatch, "r");
        len = reader.length();
        long newLength = newReader.length();
        if (newLength < len && fileReadOffset >= len) {
          if (reader != null) {
            reader.close();
          }
          
          reader = newReader;
          fileReadOffset = 0L;
          log.debug("Adaptor|"+ adaptorID + "| File size mismatched, rotating: "
              + toWatch.getAbsolutePath());
        } else {
          try {
            if (newReader != null) {
              newReader.close();
            }
            newReader =null;
          } catch (Throwable e) {
            // do nothing.
          }
        }
      } catch (IOException e) {
        // do nothing, if file doesn't exist.
      }
      if (len >= fileReadOffset) {
        if (offsetOfFirstByte > fileReadOffset) {
          // If the file rotated, the recorded offsetOfFirstByte is greater than
          // file size,
          // reset the first byte position to beginning of the file.
          fileReadOffset = 0;
          offsetOfFirstByte = 0L;
          log.warn("offsetOfFirstByte>fileReadOffset, resetting offset to 0");
        }
        hasMoreData = slurp(len, reader);

      } else {
        // file has rotated and no detection
        if (reader != null) {
          reader.close();
        }
        
        reader = null;
        fileReadOffset = 0L;
        offsetOfFirstByte = 0L;
        hasMoreData = true;
        log.warn("Adaptor|" + adaptorID + "| file: " + toWatch.getPath()
            + ", has rotated and no detection - reset counters to 0L");
      }
    } catch (IOException e) {
      log.warn("failure reading " + toWatch, e);
    }
    attempts = 0;
    adaptorInError = false;
    return hasMoreData;
  }


}
