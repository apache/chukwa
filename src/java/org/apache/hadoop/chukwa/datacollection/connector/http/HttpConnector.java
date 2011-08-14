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

package org.apache.hadoop.chukwa.datacollection.connector.http;


/**
 * This class is responsible for setting up a {@link HttpConnectorClient} with a  collectors
 * and then repeatedly calling its send function which encapsulates the work of setting up the
 * connection with the appropriate collector and then collecting and sending the {@link Chunk}s 
 * from the global {@link ChunkQueue} which where added by {@link Adaptors}. We want to separate
 * the long living (i.e. looping) behavior from the ConnectorClient because we also want to be able
 * to use the HttpConnectorClient for its add and send API in arbitrary applications that want to send
 * chunks without an {@link LocalAgent} daemon.
 * 
 * * <p>
 * On error, tries the list of available collectors, pauses for a minute, and then repeats.
 * </p>
 * <p> Will wait forever for collectors to come up. </p>

 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.ChunkQueue;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.connector.Connector;
import org.apache.hadoop.chukwa.datacollection.sender.*;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

public class HttpConnector implements Connector, Runnable {

  static Logger log = Logger.getLogger(HttpConnector.class);

  Timer statTimer = null;
  volatile int chunkCount = 0;
  
  int MAX_SIZE_PER_POST = 2 * 1024 * 1024;
  int MIN_POST_INTERVAL = 5 * 1000;
  public static final String MIN_POST_INTERVAL_OPT = "httpConnector.minPostInterval";
  public static final String MAX_SIZE_PER_POST_OPT = "httpConnector.maxPostSize";
  public static final String ASYNC_ACKS_OPT = "httpConnector.asyncAcks";

  boolean ASYNC_ACKS = false;
  
  ChunkQueue chunkQueue;

  ChukwaAgent agent;
  String argDestination = null;

  private volatile boolean stopMe = false;
  private Iterator<String> collectors = null;
  protected ChukwaSender connectorClient = null;

  { //instance initializer block
    statTimer = new Timer();
    statTimer.schedule(new TimerTask() {
      public void run() {
        int count = chunkCount;
        chunkCount = 0;
        log.info("# http chunks ACK'ed since last report: " + count);
      }
    }, 100, 60 * 1000);
  }

  public HttpConnector(ChukwaAgent agent) {
    this.agent = agent;
  }

  public HttpConnector(ChukwaAgent agent, String destination) {
    this.agent = agent;
    this.argDestination = destination;

    log.info("Setting HTTP Connector URL manually using arg passed to Agent: "
        + destination);
  }

  public void start() {

    chunkQueue = DataFactory.getInstance().getEventQueue();
    Configuration conf = agent.getConfiguration();
    MAX_SIZE_PER_POST = conf.getInt(MAX_SIZE_PER_POST_OPT, MAX_SIZE_PER_POST);
    MIN_POST_INTERVAL = conf.getInt(MIN_POST_INTERVAL_OPT, MIN_POST_INTERVAL);
    ASYNC_ACKS = conf.getBoolean(ASYNC_ACKS_OPT, ASYNC_ACKS);
    (new Thread(this, "HTTP post thread")).start();
  }

  public void shutdown() {
    stopMe = true;
    connectorClient.stop();
  }

  public void run() {
    log.info("HttpConnector started at time:" + System.currentTimeMillis());

    // build a list of our destinations from collectors
    try {
      if(collectors == null)
        collectors = DataFactory.getInstance().getCollectorURLs(agent.getConfiguration());
    } catch (IOException e) {
      log.error("Failed to retrieve list of collectors from "
          + "conf/collectors file", e);
    }
    
    if(ASYNC_ACKS) {
      try {
        connectorClient = new AsyncAckSender(agent.getConfiguration(), agent);
      } catch(IOException e) {
        log.fatal("can't read AsycAck hostlist file, exiting");
        agent.shutdown(true);
      }
    } else
      connectorClient = new ChukwaHttpSender(agent.getConfiguration());

    if (argDestination != null) {
      ArrayList<String> tmp = new ArrayList<String>();
      tmp.add(argDestination);
      collectors = tmp.iterator();
      log.info("using collector specified at agent runtime: " + argDestination);
    } else
      log.info("using collectors from collectors file");

    if (collectors == null || !collectors.hasNext()) {
      log.error("No collectors specified, exiting (and taking agent with us).");
      agent.shutdown(true);// error is unrecoverable, so stop hard.
      return;
    }

    connectorClient.setCollectors(collectors);


    try {
      long lastPost = System.currentTimeMillis();
      while (!stopMe) {
        List<Chunk> newQueue = new ArrayList<Chunk>();
        try {
          // get all ready chunks from the chunkQueue to be sent
          chunkQueue.collect(newQueue, MAX_SIZE_PER_POST); // FIXME: should
                                                           // really do this by size

        } catch (InterruptedException e) {
          System.out.println("thread interrupted during addChunks(ChunkQueue)");
          Thread.currentThread().interrupt();
          break;
        }
        int toSend = newQueue.size();
        List<ChukwaHttpSender.CommitListEntry> results = connectorClient
            .send(newQueue);
        // checkpoint the chunks which were committed
        for (ChukwaHttpSender.CommitListEntry cle : results) {
          agent.reportCommit(cle.adaptor, cle.uuid);
          chunkCount++;
        }

        long now = System.currentTimeMillis();
        long delta = MIN_POST_INTERVAL - now + lastPost;
        if(delta > 0) {
          Thread.sleep(delta); // wait for stuff to accumulate
        }
        lastPost = now;
      } // end of try forever loop
      log.info("received stop() command so exiting run() loop to shutdown connector");
    } catch (OutOfMemoryError e) {
      log.warn("Bailing out", e);
      DaemonWatcher.bailout(-1);
    } catch (InterruptedException e) {
      // do nothing, let thread die.
      log.warn("Bailing out", e);
      DaemonWatcher.bailout(-1);
    } catch (java.io.IOException e) {
      log.error("connector failed; shutting down agent");
      agent.shutdown(true);
    }
  }

  @Override
  public void reloadConfiguration() {
    Iterator<String> destinations = null;

    // build a list of our destinations from collectors
    try {
      destinations = DataFactory.getInstance().getCollectorURLs(agent.getConfiguration());
    } catch (IOException e) {
      log.error("Failed to retreive list of collectors from conf/collectors file", e);
    }
    if (destinations != null && destinations.hasNext()) {
      collectors = destinations;
      connectorClient.setCollectors(collectors);
      log.info("Resetting collectors");
    }
  }
  
  public ChukwaSender getSender() {
    return connectorClient;
  }
  
  public void setCollectors(Iterator<String> list) {
    collectors = list;
  }
}
