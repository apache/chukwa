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

package org.apache.hadoop.chukwa.datacollection;


import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Iterator;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.agent.MemLimitQueue;
import org.apache.hadoop.chukwa.datacollection.sender.RetryListOfCollectors;
import org.apache.log4j.Logger;

public class DataFactory {
  static Logger log = Logger.getLogger(DataFactory.class);
  static final String COLLECTORS_FILENAME = "collectors";
  static final String CHUNK_QUEUE = "chukwaAgent.chunk.queue";
  
  protected static final DataFactory dataFactory = new DataFactory();
  private ChunkQueue chunkQueue = null;

  private String defaultTags = "";
  
  private DataFactory() {
  }

  public static DataFactory getInstance() {
    return dataFactory;
  }

  public synchronized ChunkQueue getEventQueue() {
    if (chunkQueue == null) {
      chunkQueue = createEventQueue();
    }
    return chunkQueue;
  }

  public void put(Chunk c) throws InterruptedException {
    chunkQueue.add(c);
  }

  public synchronized ChunkQueue createEventQueue() {
    Configuration conf = ChukwaAgent.getStaticConfiguration();
    if(conf == null){
    //Must be a unit test, use default queue with default configuration
      return new MemLimitQueue(null);
    }
    String receiver = conf.get(CHUNK_QUEUE);
    ChunkQueue queue = null;
    if(receiver == null){
      log.warn("Empty configuration for " + CHUNK_QUEUE + ". Defaulting to MemLimitQueue");
      queue = new MemLimitQueue(conf);
      return queue;
    }
    
    try {
      Class<?> clazz = Class.forName(receiver);
      log.info(clazz);
      if(!ChunkQueue.class.isAssignableFrom(clazz)){
        throw new Exception(receiver + " is not an instance of ChunkQueue");
      }
      try {
        Constructor<?> ctor = clazz.getConstructor(new Class[]{Configuration.class});
        queue = (ChunkQueue) ctor.newInstance(conf);
      } catch(NoSuchMethodException nsme){
        //Queue implementations which take no configuration parameter
        queue = (ChunkQueue) clazz.newInstance();
      }
    } catch(Exception e) {
      log.error("Could not instantiate configured ChunkQueue due to: " + e);
      log.error("Defaulting to MemLimitQueue");
      queue = new MemLimitQueue(conf);
    }
    return queue;
  }

  public String getDefaultTags() {
    return defaultTags;
  }

  public void setDefaultTags(String tags) {
    defaultTags = tags;
  }

  public void addDefaultTag(String tag) {
    this.defaultTags += " " + tag.trim();
  }
  
  /**
   * @return empty list if file does not exist
   * @throws IOException on other error
   */
  public Iterator<String> getCollectorURLs(Configuration conf, String filename) throws IOException {
    String chukwaHome = System.getenv("CHUKWA_HOME");
    if (chukwaHome == null) {
      chukwaHome = ".";
    }

    if (!chukwaHome.endsWith("/")) {
      chukwaHome = chukwaHome + File.separator;
    }
    log.info("Config - System.getenv(\"CHUKWA_HOME\"): [" + chukwaHome + "]");

    String chukwaConf = System.getenv("CHUKWA_CONF_DIR");
    if (chukwaConf == null) {
      chukwaConf = chukwaHome + "conf" + File.separator;
    }

    log.info("Config - System.getenv(\"chukwaConf\"): [" + chukwaConf + "]");

    log.info("setting up collectors file: " + chukwaConf + File.separator
        + COLLECTORS_FILENAME);
    File collectors = new File(chukwaConf + File.separator + filename);
    try {
      return new RetryListOfCollectors(collectors, conf);
    } catch (java.io.IOException e) {
      log.error("failed to read collectors file: ", e);
      throw e;
    }
  }
  public Iterator<String> getCollectorURLs(Configuration conf) throws IOException {
    return getCollectorURLs(conf, "collectors");
  }

}
