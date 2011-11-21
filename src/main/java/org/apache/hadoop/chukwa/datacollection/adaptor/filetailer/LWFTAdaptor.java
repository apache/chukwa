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
import org.apache.hadoop.chukwa.datacollection.adaptor.AbstractAdaptor;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorException;
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * A base class for file tailing adaptors.  
 * Intended to mandate as little policy as possible, and to use as 
 * few system resources as possible.
 * 
 * 
 * If the file does not exist, this class will continue to retry quietly
 * forever and will start tailing if it's eventually created.
 */
public class LWFTAdaptor extends AbstractAdaptor {
  
  /**
   * This is the maximum amount we'll read from any one file before moving on to
   * the next. This way, we get quick response time for other files if one file
   * is growing rapidly.
   * 
   */
  public static final int DEFAULT_MAX_READ_SIZE = 128 * 1024;
  public static final String MAX_READ_SIZE_OPT = 
      "chukwaAgent.fileTailingAdaptor.maxReadSize";

  public static int MAX_READ_SIZE = DEFAULT_MAX_READ_SIZE;
  
  static Logger log;
  protected static FileTailer tailer;
  
  static {
    tailer = null;
    log = Logger.getLogger(FileTailingAdaptor.class);
  }
  
  
  /**
   * next PHYSICAL offset to read
   */
  protected long fileReadOffset;

  /**
   * The logical offset of the first byte of the file
   */
  protected long offsetOfFirstByte = 0;
  protected Configuration conf = null;
  
  File toWatch;

  @Override
  public void start(long offset) {
    synchronized(LWFTAdaptor.class) {
      if(tailer == null)
        tailer = new FileTailer(control.getConfiguration());
    }
    this.fileReadOffset = offset - offsetOfFirstByte;    
    tailer.startWatchingFile(this);
  }
  
  /**
   * @see org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor#getCurrentStatus()
   */
  public String getCurrentStatus() {
    return type.trim() + " " + offsetOfFirstByte + " " + toWatch.getPath();
  }

  public String toString() {
    return "Lightweight Tailer on " + toWatch;
  }

  public String getStreamName() {
    return toWatch.getPath();
  }
  
  @Override
  public String parseArgs(String params) { 
    conf = control.getConfiguration();
    MAX_READ_SIZE = conf.getInt(MAX_READ_SIZE_OPT, DEFAULT_MAX_READ_SIZE);

    Pattern cmd = Pattern.compile("(\\d+)\\s+(.+)\\s?");
    Matcher m = cmd.matcher(params);
    if (m.matches()) { //check for first-byte offset. If absent, assume we just got a path.
      offsetOfFirstByte = Long.parseLong(m.group(1));
      toWatch = new File(m.group(2));
    } else {
      toWatch = new File(params.trim());
    }
    return toWatch.getAbsolutePath();
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    tailer.stopWatchingFile(this);
    return fileReadOffset + offsetOfFirstByte;
  }
  

  /**
   * Extract records from a byte sequence
   * 
   * @param eq the queue to stick the new chunk[s] in
   * @param buffOffsetInFile the byte offset in the stream at which buf[] begins
   * @param buf the byte buffer to extract records from
   * @return the number of bytes processed
   * @throws InterruptedException
   */
  protected int extractRecords(ChunkReceiver eq, long buffOffsetInFile,
      byte[] buf) throws InterruptedException {
    if(buf.length == 0)
      return 0;
    
    ChunkImpl chunk = new ChunkImpl(type, toWatch.getAbsolutePath(),
        buffOffsetInFile + buf.length, buf, this);

    eq.add(chunk);
    return buf.length;
  }
  
  protected boolean slurp(long len, RandomAccessFile reader) throws IOException,
  InterruptedException{
    boolean hasMoreData = false;

    log.debug("Adaptor|" + adaptorID + "|seeking|" + fileReadOffset);
    reader.seek(fileReadOffset);

    long bufSize = len - fileReadOffset;

   if (bufSize > MAX_READ_SIZE) {
      bufSize = MAX_READ_SIZE;
      hasMoreData = true;
    }
    byte[] buf = new byte[(int) bufSize];

    long curOffset = fileReadOffset;

    int bufferRead = reader.read(buf);
    assert reader.getFilePointer() == fileReadOffset + bufSize : " event size arithmetic is broken: "
        + " pointer is "
        + reader.getFilePointer()
        + " but offset is "
        + fileReadOffset + bufSize;

    int bytesUsed = extractRecords(dest,
        fileReadOffset + offsetOfFirstByte, buf);

    // === WARNING ===
    // If we couldn't found a complete record AND
    // we cannot read more, i.e bufferRead == MAX_READ_SIZE
    // it's because the record is too BIG
    // So log.warn, and drop current buffer so we can keep moving
    // instead of being stopped at that point for ever
    if (bytesUsed == 0 && bufferRead == MAX_READ_SIZE) {
      log.warn("bufferRead == MAX_READ_SIZE AND bytesUsed == 0, dropping current buffer: startOffset="
              + curOffset
              + ", MAX_READ_SIZE="
              + MAX_READ_SIZE
              + ", for "
              + toWatch.getPath());
      bytesUsed = buf.length;
    }

    fileReadOffset = fileReadOffset + bytesUsed;

    log.debug("Adaptor|" + adaptorID + "|start|" + curOffset + "|end|"
        + fileReadOffset);
    return hasMoreData;
  }
  
  public synchronized boolean tailFile()
  throws InterruptedException {
    boolean hasMoreData = false;
    try {
      
       //if file doesn't exist, length =0 and we just keep waiting for it.
      //if(!toWatch.exists())
      //  deregisterAndStop(false);
      
      long len = toWatch.length();
      if(len < fileReadOffset) {
        //file shrank; probably some data went missing.
        handleShrunkenFile(len);
      } else if(len > fileReadOffset) {
        RandomAccessFile reader = new RandomAccessFile(toWatch, "r");
        slurp(len, reader);
        reader.close();
      }
    } catch(IOException e) {
      log.warn("IOException in tailer", e);
      deregisterAndStop();
    }
    
    return hasMoreData;
  }

  private void handleShrunkenFile(long measuredLen) {
    log.info("file "+ toWatch +"shrank from " + fileReadOffset + " to " + measuredLen);
    offsetOfFirstByte = measuredLen;
    fileReadOffset = 0;
  }

}
