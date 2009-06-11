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

package org.apache.hadoop.chukwa.tools.backfilling;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class QueueToWriterConnector implements Connector, Runnable {
  static Logger log = Logger.getLogger(QueueToWriterConnector.class);
  static final int MAX_SIZE_PER_POST = 2 * 1024 * 1024;

  protected Configuration conf = null;
  protected volatile boolean isRunning = true;
  protected ChunkQueue chunkQueue = DataFactory.getInstance().getEventQueue();
  protected ChukwaWriter writer = null;
  protected Thread runner = null;
  protected boolean isBackfilling = false;
  public QueueToWriterConnector(Configuration conf,boolean isBackfilling) {
    this.conf = conf;
    this.isBackfilling = isBackfilling;
  }

  @Override
  public void reloadConfiguration() {
    // do nothing here
  }

  @Override
  public void shutdown() {
    isRunning = false;
    
    log.info("Shutdown in progress ...");
    while (isAlive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {}
    }
   
    try {
      if (writer != null) {
        writer.close();
      }
    } catch(Exception e) {
      log.warn("Exception while closing writer: ", e);
    }
    log.info("Shutdown done.");
  }

  @Override
  public void start() {
    log.info("Starting QueueToWriterConnector thread");
    runner = new Thread(this, "QueueToWriterConnectorThread");
    runner.start();
  }

  protected boolean isAlive() {
    return this.runner.isAlive();
  }
  
  @Override
  public void run() {

    log.info("initializing QueueToWriterConnector");
    try {
      String writerClassName = conf.get("chukwaCollector.writerClass",
          SeqFileWriter.class.getCanonicalName());
      Class<?> writerClass = Class.forName(writerClassName);
      if (writerClass != null
          && ChukwaWriter.class.isAssignableFrom(writerClass)) {
        writer = (ChukwaWriter) writerClass.newInstance();
      } else {
        throw new RuntimeException("Wrong class type");
      }
      writer.init(conf);

    } catch (Throwable e) {
      log.warn("failed to use user-chosen writer class, Bail out!", e);
      DaemonWatcher.bailout(-1);
    }

    
    List<Chunk> chunks = new LinkedList<Chunk>();
    ChukwaAgent agent = null;// ChukwaAgent.getAgent();
    
    log.info("processing data for QueueToWriterConnector");
    
    while ( isRunning ||  chunkQueue.size() != 0 || chunks.size() != 0) {
      try {
        if (chunks.size() == 0) {
          
          if (isBackfilling && chunkQueue.size() == 0) {
            Thread.sleep(300);
            continue;
          }
          chunkQueue.collect(chunks, MAX_SIZE_PER_POST);
          log.info("Got " + chunks.size() + " chunks back from the queue");
        }       
        
        writer.add(chunks);
        
        if (agent != null) {
          for(Chunk chunk: chunks) {
            agent.reportCommit(chunk.getInitiator(), chunk.getSeqID());
          }
        }
        
        chunks.clear();
        
      }
      catch (Throwable e) {
        log.warn("Could not save some chunks");
        e.printStackTrace();
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e1) {}
      } 
    }
  }

}
