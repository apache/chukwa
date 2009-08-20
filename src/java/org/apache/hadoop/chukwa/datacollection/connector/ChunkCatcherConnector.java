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
package org.apache.hadoop.chukwa.datacollection.connector;


import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.*;
import java.util.*;

public class ChunkCatcherConnector implements Connector {

  ChunkQueue eq;
  
  Timer tm;
  
  class Interruptor extends TimerTask {
    Thread targ;
    volatile boolean deactivate = false;
    Interruptor(Thread t) {
      targ =t;
    }
    
    public synchronized void run() {
      if(!deactivate)
        targ.interrupt();
    }
  };

  public void start() {
    eq = DataFactory.getInstance().getEventQueue();
    tm = new Timer();
  }

  public Chunk waitForAChunk(long ms) {
    
    ArrayList<Chunk> chunks = new ArrayList<Chunk>();
    Interruptor i = new Interruptor(Thread.currentThread());
    if(ms > 0)
      tm.schedule(i, ms);
    try {
      eq.collect(chunks, 1);
      synchronized(i) {
        i.deactivate = true;
      }
    } catch(InterruptedException e) {
      Thread.interrupted();
      return null;
    }
    return chunks.get(0);
  }
  
  public Chunk waitForAChunk() throws InterruptedException {
    return this.waitForAChunk(0);//wait forever by default
  }

  public void shutdown() {
    tm.cancel();
  }

  @Override
  public void reloadConfiguration() {
    System.out.println("reloadConfiguration");
  }
  
  public void clear() throws InterruptedException {
    ArrayList<Chunk> list = new ArrayList<Chunk>();
    while(eq.size() > 0)
      eq.collect(list, 1);
  }

}
