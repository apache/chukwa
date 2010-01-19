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

import java.util.*;
import java.io.*;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.log4j.Logger;
import static org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy.*;

public class WriteaheadBuffered extends AbstractWrapper {
  Logger log = Logger.getLogger(WriteaheadBuffered.class);
  static final String BUF_DIR_OPT = "adaptor.writeaheadWrapper.dir";
  static  String BUF_DIR = "/tmp"; //1 MB
  static long COMPACT_AT = 1024 * 1024; //compact when it can free at least this much storage
  
  File outBuf;
  DataOutputStream outToDisk;
  long fSize, highestSentOffset;
  
  
  @Override
  public synchronized void add(Chunk event) throws InterruptedException {
    try {
      event.write(outToDisk);
      outToDisk.flush();
      fSize += event.getData().length;
      long seq = event.getSeqID();
      if(seq > highestSentOffset)
        highestSentOffset = seq;
    } catch(IOException e) {
      log.error(e);
    }
    dest.add(event);
  }
  
  @Override
  public void start(String adaptorID, String type, long offset,
      ChunkReceiver dest) throws AdaptorException {
    try {
      String dummyAdaptorID = adaptorID;
      this.dest = dest;
      
      outBuf = new File(BUF_DIR, adaptorID);
      long newOffset = offset;
      if(outBuf.length() > 0) {
        DataInputStream dis = new DataInputStream(new FileInputStream(outBuf));
        while(dis.available() > 0) {
          Chunk c = ChunkImpl.read(dis);
          fSize += c.getData().length;
          long seq = c.getSeqID();
          if(seq >offset) {
            dest.add(c);
            newOffset = seq;
          }
        }
        //send chunks that are outstanding        
        dis.close();
      }
      outToDisk = new DataOutputStream(new FileOutputStream(outBuf, true));
      
      inner.start(dummyAdaptorID, innerType, newOffset, this);
    } catch(IOException e) {
      throw new AdaptorException(e);
    } catch(InterruptedException e) {
      throw new AdaptorException(e);
    }
  }
  
  @Override
  public synchronized void committed(long l) {

    try {
      long bytesOutstanding = highestSentOffset - l;
      if(fSize - bytesOutstanding > COMPACT_AT) {
        fSize = 0;
        outToDisk.close();
        File outBufTmp = new File(outBuf.getAbsoluteFile(), outBuf.getName() + ".tmp");
        outBuf.renameTo(outBufTmp);
        outToDisk = new DataOutputStream(new FileOutputStream(outBuf, false));
        DataInputStream dis = new DataInputStream(new FileInputStream(outBufTmp));
        while(dis.available() > 0) {
          Chunk c = ChunkImpl.read(dis);
          if(c.getSeqID() > l) { //not yet committed
            c.write(outToDisk);
            fSize += c.getData().length;
          }
        }
        dis.close();
        outBufTmp.delete();
      }
    } catch(IOException e) {
      log.error(e);
      //should this be fatal?
    }
  }
  
  @Override
  public long shutdown(AdaptorShutdownPolicy p) throws AdaptorException {
    if(p != RESTARTING)
      outBuf.delete();    
    return inner.shutdown(p);
  }

}
