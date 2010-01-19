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

import static org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy.RESTARTING;
import java.util.*;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkReceiver;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;

public class MemBuffered extends AbstractWrapper {
  
  static final String BUF_SIZE_OPT = "adaptor.memBufWrapper.size";
  static final int DEFAULT_BUF_SIZE = 1024*1024; //1 MB
  
  //true by default. If you were willing to discard data, you didn't need Mem Buffers
  static boolean BLOCK_WHEN_FULL = true;
  
  static class MemBuf {
    long dataSizeBytes;
    final long maxDataSize;
    final ArrayDeque<Chunk> chunks;
    
    public MemBuf(long maxDataSize) {
      dataSizeBytes = 0;
      this.maxDataSize = maxDataSize;
      chunks = new ArrayDeque<Chunk>();
    }
    
    synchronized void add(Chunk c) throws InterruptedException{
      int len = c.getData().length;
      if(BLOCK_WHEN_FULL)
        while(len + dataSizeBytes > maxDataSize)
          wait();
      else
        chunks.remove();
      dataSizeBytes += len;
      chunks.add(c);
    }
    
    synchronized void removeUpTo(long l) {

      long bytesFreed = 0;
      while(!chunks.isEmpty()) {
        Chunk c = chunks.getFirst();
        if(c.getSeqID() > l)
          chunks.addFirst(c);
        else
          bytesFreed += c.getData().length;
      }
      
      if(bytesFreed > 0) {
        dataSizeBytes -= bytesFreed;
        notifyAll();
      }
    }
    
  }

  static Map<String, MemBuf> buffers;
  static {
    buffers = new HashMap<String, MemBuf>();
  }
  
  MemBuf myBuffer;
  
  @Override
  public void add(Chunk event) throws InterruptedException {
    myBuffer.add(event);
    dest.add(event);
  }
  
  @Override
  public void start(String adaptorID, String type, long offset,
      ChunkReceiver dest) throws AdaptorException {
    try {
      String dummyAdaptorID = adaptorID;
      this.adaptorID = adaptorID;
      this.dest = dest;
      
      long bufSize = manager.getConfiguration().getInt(BUF_SIZE_OPT, DEFAULT_BUF_SIZE);
      synchronized(buffers) {
        myBuffer = buffers.get(adaptorID);
        if(myBuffer == null) {
          myBuffer = new MemBuf(bufSize);
          buffers.put(adaptorID, myBuffer);
        }
      }

      //Drain buffer into output queue
      long offsetToStartAt = offset;
      for(Chunk c:myBuffer.chunks) {
        dest.add(c);
        long seq = c.getSeqID();
        if(seq > offsetToStartAt)
          offsetToStartAt = seq;
      }
      
      inner.start(dummyAdaptorID, innerType, offsetToStartAt, this);
    } catch(InterruptedException e) {
     throw new AdaptorException(e);
    }
  }
  
  @Override
  public void committed(long l) {
    myBuffer.removeUpTo(l);
  }
  
  @Override
  public long shutdown(AdaptorShutdownPolicy p) throws AdaptorException {
    if(p != RESTARTING)
      buffers.remove(adaptorID);    
    return inner.shutdown(p);
  }


}
