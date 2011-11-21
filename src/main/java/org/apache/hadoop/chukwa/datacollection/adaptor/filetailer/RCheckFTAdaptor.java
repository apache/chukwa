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
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.Queue;
import java.util.LinkedList;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;

/**
 * Checkpoint state:
 *   date modified of most-recently tailed file, offset of first byte of that file,
 *   then regular FTA arts 
 *
 */
public class RCheckFTAdaptor extends LWFTAdaptor implements FileFilter {
  
  private static class FPair implements Comparable<FPair> {
    File f;
    long mod;
    FPair(File f) {
      this.f = f;
      mod = f.lastModified();
    }
    /**
     * -1   implies this is LESS THAN o
     */
    @Override
    public int compareTo(FPair o) {
      if(mod < o.mod)
        return -1;
      else if (mod > o.mod)
        return 1;
      //want toWatch to be last after a rotation; otherwise, this is basically 
      //just a heuristic that hasn't been tuned yet
      else return (o.f.getName().compareTo(f.getName()));
    }
  }
  
  long prevFileLastModDate = 0;
  LinkedList<FPair> fileQ = new LinkedList<FPair>(); 
  String fBaseName;
  File cur; //this is the actual physical file being watched.  
            // in contrast, toWatch is the path specified by the user
  boolean caughtUp = false;
  /**
   * Check for date-modified and offset; if absent assume we just got a name.
   */
  @Override
  public String parseArgs(String params) { 
    Pattern cmd = Pattern.compile("d:(\\d+)\\s+(\\d+)\\s+(.+)\\s?");
    Matcher m = cmd.matcher(params);
    if (m.matches()) {
      prevFileLastModDate = Long.parseLong(m.group(1));
      offsetOfFirstByte = Long.parseLong(m.group(2));
      toWatch = new File(m.group(3)).getAbsoluteFile();
    } else {
      toWatch = new File(params.trim()).getAbsoluteFile();
    }
    fBaseName = toWatch.getName();
    return toWatch.getAbsolutePath();
  }
  
  public String getCurrentStatus() {
    return type.trim() + " d:" + prevFileLastModDate + " "  + offsetOfFirstByte + " " + toWatch.getPath();
  }

  @Override
  public boolean accept(File pathname) {
    return pathname.getName().startsWith(fBaseName) &&
     ( pathname.getName().equals(fBaseName) ||
       pathname.lastModified() > prevFileLastModDate);
  }
  
  
  protected void mkFileQ() {
    
    File toWatchDir = toWatch.getParentFile();
    File[] candidates = toWatchDir.listFiles(this);
    if(candidates == null) {
      log.error(toWatchDir + " is not a directory in "+adaptorID);
    } else {
      log.debug("saw " + candidates.length + " files matching pattern");
      fileQ = new LinkedList<FPair>();
      for(File f:candidates)
        fileQ.add(new FPair(f));
      Collections.sort(fileQ);
    } 
  }
  
  protected void advanceQ() {
    FPair next = fileQ.poll();
    if(next != null) {
      cur = next.f;
      caughtUp = toWatch.equals(cur);
      if(caughtUp && !fileQ.isEmpty()) 
        log.warn("expected rotation queue to be empty when caught up...");
    }
    else {
      cur = null;
      caughtUp = true;
    }
  }
  
  @Override
  public void start(long offset) {
    mkFileQ(); //figure out what to watch
    advanceQ();
    super.start(offset);
  }
  
  @Override
  public synchronized boolean tailFile()
  throws InterruptedException {
    boolean hasMoreData = false;
    try {
      
      if(caughtUp) {
        //we're caught up and watching an unrotated file
        mkFileQ(); //figure out what to watch
        advanceQ();
      }
      if(cur == null) //file we're watching doesn't exist
        return false;
      
      
      long len = cur.length();
      long tsPreTail = cur.exists() ? cur.lastModified() : prevFileLastModDate;

      if(log.isDebugEnabled())
        log.debug(adaptorID + " treating " + cur + " as " + toWatch + " len = " + len);
      
      if(len < fileReadOffset) {
        log.info("file "+ cur +" shrank from " + fileReadOffset + " to " + len);
        //no unseen changes to prev version, since mod date is older than last scan.
        offsetOfFirstByte += fileReadOffset;
        fileReadOffset = 0;
      } else if(len > fileReadOffset) {
        log.debug("slurping from " + cur+ " at offset " + fileReadOffset);
        RandomAccessFile reader = new RandomAccessFile(cur, "r");
        slurp(len, reader);
        reader.close();
      } else {
        //we're either caught up or at EOF
        if (!caughtUp) {
          prevFileLastModDate = cur.lastModified();
          //Hit EOF on an already-rotated file. Move on!
          offsetOfFirstByte += fileReadOffset;
          fileReadOffset = 0;
          advanceQ();
          log.debug("not caught up, and hit EOF.  Moving forward in queue to " + cur);
        } else 
          prevFileLastModDate = tsPreTail;

      }
        
    } catch(IOException e) {
      log.warn("IOException in "+adaptorID, e);
      deregisterAndStop();
    }
    
    return hasMoreData;
  }
  

  public String toString() {
    return "Rotation-aware Tailer on " + toWatch;
  }
}
