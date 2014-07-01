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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent.AlreadyRunningException;
import org.apache.hadoop.conf.Configuration;

import junit.framework.TestCase;

public class TestChunkQueue extends TestCase {

  private final byte[] data = "this is a chunk".getBytes();
  List<Chunk> putList, getList;
  int NUM_CHUNKS = 10;
  int QUEUE_SIZE = 6;
  int QUEUE_LIMIT = data.length * QUEUE_SIZE;
  ChukwaAgent agent = null;
  Configuration conf = null;
  final String CHUNK_QUEUE_LIMIT = "chukwaAgent.chunk.queue.limit";
  final String CHUNK_QUEUE = "chukwaAgent.chunk.queue";
  DataFactory df = DataFactory.getInstance();
  
  @Override
  protected void setUp() throws AlreadyRunningException {
    agent = ChukwaAgent.getAgent();
    if(agent == null){
      agent = new ChukwaAgent();
    }
    conf = agent.getConfiguration();
    conf.set(CHUNK_QUEUE_LIMIT, Integer.toString(QUEUE_LIMIT));
    putList = new ArrayList<Chunk>(10);
    for (int i = 1; i <= NUM_CHUNKS; i++) {
      Chunk c = new ChunkImpl("DataType", "StreamName", (long) i, data, null);
      putList.add(c);
    }
  }
  
  @Override 
  protected void tearDown() {
    if(agent != null){
      agent.shutdown();
    }
  }
  
  public void testMemLimitQueue() {
    conf.set(CHUNK_QUEUE, "org.apache.hadoop.chukwa.datacollection.agent.MemLimitQueue");
    ChunkQueue mlq = df.createEventQueue();
    testBlockingNature(mlq);
  }

  public void testNonBlockingMemLimitQueue() {
    conf.set(CHUNK_QUEUE, "org.apache.hadoop.chukwa.datacollection.agent.NonBlockingMemLimitQueue");
    ChunkQueue nbmlq = df.createEventQueue();
    testNonBlockingNature(nbmlq);
  }

  /**
   * Putter thread gets a list of chunks and adds all of them
   * to the ChunkQueue
   */
  private class Putter extends Thread {
    List<Chunk> chunks;
    ChunkQueue q;

    Putter(List<Chunk> chunks, ChunkQueue q) {
      this.chunks = chunks;
      this.q = q;
    }

    public void run() {
      try {
        for (Chunk c : chunks) {
          q.add(c);
        }
      } catch (InterruptedException e) {
      }
    }
  }

  /**
   * Getter thread collects all the chunks from the 
   * ChunkQueue indefinitely
   */
  private class Getter extends Thread {
    List<Chunk> chunks;
    ChunkQueue q;

    Getter(List<Chunk> chunks, ChunkQueue q) {
      this.chunks = chunks;
      this.q = q;
    }

    public void run() {
      try {
        while (true) {
          q.collect(chunks, Integer.MAX_VALUE);
        }
      } catch (InterruptedException e) {
      }
    }
  }

  private void joinThread(Thread t, int timeout) {
    try {
      t.join(timeout);
    } catch (InterruptedException e) {
    }
  }

  /**
   * This test makes sure that the putter thread blocks when queue is full
   * 
   * @param ChunkQueue
   *          q
   */
  private void testBlockingNature(ChunkQueue q) {
    Putter putter = new Putter(putList, q);
    putter.start();
    joinThread(putter, 3000);
    if (!putter.isAlive()) {
      fail("Blocking queue semantics not implemented");
    }
    assertTrue("Could not verify queue size after put", q.size() == QUEUE_SIZE);
    getList = new ArrayList<Chunk>();
    Getter getter = new Getter(getList, q);
    getter.start();
    joinThread(getter, 3000);
    assertTrue("Could not verify queue size after get", q.size() == 0);
    // make sure we got all chunks
    assertTrue("Could not verify all chunks got drained after get",
        getList.size() == NUM_CHUNKS);
    putter.interrupt();
    getter.interrupt();
  }

  /**
   * This test makes sure that the putter thread does not blocks when queue is
   * full. This test does not check if the queue implementation uses a circular
   * buffer to retain the most recent chunks or discards new incoming chunks
   * 
   * @param ChunkQueue
   *          q
   */
  private void testNonBlockingNature(ChunkQueue q) {
    Putter putter = new Putter(putList, q);
    putter.start();
    joinThread(putter, 3000);
    if (putter.isAlive()) {
      fail("Non Blocking queue semantics not implemented");
    }
    assertTrue("Could not verify queue size after put", q.size() == QUEUE_SIZE);
    getList = new ArrayList<Chunk>();
    Getter getter = new Getter(getList, q);
    getter.start();
    joinThread(getter, 3000);
    assertTrue("Could not verify all chunks got drained after get",
        q.size() == 0);
    // make sure we got only the chunks
    assertTrue("Could not verify chunks after get",
        getList.size() == QUEUE_SIZE);
    putter.interrupt();
    getter.interrupt();
  }
}
