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

package org.apache.hadoop.chukwa.datacollection.agent;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.agent.metrics.ChunkQueueMetrics;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * An event queue that discards incoming chunks once a fixed upper limit of data
 * is enqueued. The method calling add will not block.
 * 
 * For now, uses the size of the data field. Should really use
 * estimatedSerializedSize()?
 * 
 */
public class NonBlockingMemLimitQueue implements ChunkQueue {
  static Logger log = Logger.getLogger(NonBlockingMemLimitQueue.class);
  static final ChunkQueueMetrics metrics = new ChunkQueueMetrics("chukwaAgent",
      "chunkQueue");
  static final String CHUNK_QUEUE_LIMIT = "chukwaAgent.chunk.queue.limit";
  static final int QUEUE_SIZE = 10 * 1024 * 1024;
  private Queue<Chunk> queue = new LinkedList<Chunk>();
  private long dataSize = 0;
  private long MAX_MEM_USAGE;

  public NonBlockingMemLimitQueue(Configuration conf) {
    configure(conf);
  }
  
  /**
   * @see org.apache.hadoop.chukwa.datacollection.ChunkQueue#add(org.apache.hadoop.chukwa.Chunk)
   */
  public void add(Chunk chunk) throws InterruptedException {
    assert chunk != null : "can't enqueue null chunks";
    int chunkSize = chunk.getData().length;
    synchronized (this) {
      if (chunkSize + dataSize > MAX_MEM_USAGE) {
        if (dataSize == 0) { // queue is empty, but data is still too big
          log.error("JUMBO CHUNK SPOTTED: type= " + chunk.getDataType()
              + " and source =" + chunk.getStreamName());
          return; // return without sending; otherwise we'd deadlock.
          // this error should probably be fatal; there's no way to
          // recover.
        } else {
          metrics.fullQueue.set(1);
          log.warn("Discarding chunk due to NonBlockingMemLimitQueue full [" + dataSize
              + "]");
          return;
        }
      }
      metrics.fullQueue.set(0);
      dataSize += chunk.getData().length;
      queue.add(chunk);
      metrics.addedChunk.inc();
      metrics.queueSize.set(queue.size());
      metrics.dataSize.set(dataSize);
      this.notifyAll();
    }
  }

  /**
   * @see org.apache.hadoop.chukwa.datacollection.ChunkQueue#collect(java.util.List,
   *      int)
   */
  public void collect(List<Chunk> events, int maxSize)
      throws InterruptedException {
    synchronized (this) {
      // we can't just say queue.take() here, since we're holding a lock.
      while (queue.isEmpty()) {
        this.wait();
      }

      int size = 0;
      while (!queue.isEmpty() && (size < maxSize)) {
        Chunk e = this.queue.remove();
        metrics.removedChunk.inc();
        int chunkSize = e.getData().length;
        size += chunkSize;
        dataSize -= chunkSize;
        metrics.dataSize.set(dataSize);
        events.add(e);
      }
      metrics.queueSize.set(queue.size());
      this.notifyAll();
    }

    if (log.isDebugEnabled()) {
      log.debug("WaitingQueue.inQueueCount:" + queue.size()
          + "\tWaitingQueue.collectCount:" + events.size());
    }
  }

  public int size() {
    return queue.size();
  }

  private void configure(Configuration conf) {
    MAX_MEM_USAGE = QUEUE_SIZE;
    if(conf == null){
      return;
    }
    String limit = conf.get(CHUNK_QUEUE_LIMIT);
    if(limit != null){
      try{
        MAX_MEM_USAGE = Integer.parseInt(limit);
      } catch(NumberFormatException nfe) {
        log.error("Exception reading property " + CHUNK_QUEUE_LIMIT
            + ". Defaulting internal queue size to " + QUEUE_SIZE);
      }
    }
    log.info("Using NonBlockingMemLimitQueue limit of " + MAX_MEM_USAGE);
  }
}
