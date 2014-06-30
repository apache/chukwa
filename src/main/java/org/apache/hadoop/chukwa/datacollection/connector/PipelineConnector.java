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

/**
 * This class is responsible for setting up connections with configured
 * storage writers base on configuration of chukwa_agent.xml.
 * 
 * On error, tries the list of available storage writers, pauses for a minute, 
 * and then repeats.
 *
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter.CommitStatus;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineStageWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class PipelineConnector implements Connector, Runnable {

  static Logger log = Logger.getLogger(PipelineConnector.class);

  Timer statTimer = null;
  volatile int chunkCount = 0;
  
  int MAX_SIZE_PER_POST = 2 * 1024 * 1024;
  int MIN_POST_INTERVAL = 5 * 1000;
  public static final String MIN_POST_INTERVAL_OPT = "pipelineConnector.minPostInterval";
  public static final String MAX_SIZE_PER_POST_OPT = "pipelineConnector.maxPostSize";
  public static final String ASYNC_ACKS_OPT = "pipelineConnector.asyncAcks";

  ChunkQueue chunkQueue;

  private static volatile ChukwaAgent agent = null;

  private volatile boolean stopMe = false;
  protected ChukwaWriter writers = null;

  public PipelineConnector() {
    //instance initializer block
    statTimer = new Timer();
    statTimer.schedule(new TimerTask() {
      public void run() {
        int count = chunkCount;
        chunkCount = 0;
        log.info("# Data chunks sent since last report: " + count);
      }
    }, 100, 60 * 1000);
  }
  
  public void start() {
    chunkQueue = DataFactory.getInstance().getEventQueue();
    agent = ChukwaAgent.getAgent();
    Configuration conf = agent.getConfiguration();
    MAX_SIZE_PER_POST = conf.getInt(MAX_SIZE_PER_POST_OPT, MAX_SIZE_PER_POST);
    MIN_POST_INTERVAL = conf.getInt(MIN_POST_INTERVAL_OPT, MIN_POST_INTERVAL);
    try {
      writers = new PipelineStageWriter(conf);
      (new Thread(this, "Pipeline connector thread")).start();
    } catch(Exception e) {
      log.error("Pipeline initialization error: ", e);
    }
  }

  public void shutdown() {
    stopMe = true;
    try {
      writers.close();
    } catch (WriterException e) {
      log.warn("Shutdown error: ",e);
    }
  }

  public void run() {
    log.info("PipelineConnector started at time:" + System.currentTimeMillis());

    try {
      long lastPost = System.currentTimeMillis();
      while (!stopMe) {
        List<Chunk> newQueue = new ArrayList<Chunk>();
        try {
          // get all ready chunks from the chunkQueue to be sent
          chunkQueue.collect(newQueue, MAX_SIZE_PER_POST);
        } catch (InterruptedException e) {
          log.warn("thread interrupted during addChunks(ChunkQueue)");
          Thread.currentThread().interrupt();
          break;
        }
        CommitStatus result = writers.add(newQueue);
        if(result.equals(ChukwaWriter.COMMIT_OK)) {
          chunkCount = newQueue.size();
          for (Chunk c : newQueue) {
            agent.reportCommit(c.getInitiator(), c.getSeqID());
          }          
        }
        long now = System.currentTimeMillis();
        long delta = MIN_POST_INTERVAL - now + lastPost;
        if(delta > 0) {
          Thread.sleep(delta); // wait for stuff to accumulate
        }
        lastPost = now;
      } // end of try forever loop
      log.info("received stop() command so exiting run() loop to shutdown connector");
    } catch (WriterException e) {
      log.warn("PipelineStageWriter Exception: ", e);
    } catch (OutOfMemoryError e) {
      log.warn("Bailing out", e);
      DaemonWatcher.bailout(-1);
    } catch (InterruptedException e) {
      // do nothing, let thread die.
      log.warn("Bailing out", e);
      DaemonWatcher.bailout(-1);
    } catch (Throwable e) {
      log.error("connector failed; shutting down agent: ", e);
      throw new RuntimeException("Shutdown pipeline connector.");
    }
  }

  @Override
  public void reloadConfiguration() {
  }
  
}
