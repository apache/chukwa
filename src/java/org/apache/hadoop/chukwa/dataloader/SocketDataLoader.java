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
package org.apache.hadoop.chukwa.dataloader;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.writer.SocketTeeWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;

/**
 * Socket Data Loader, also known as the SDL, is a framework for allowing direct
 * access to log data under the Chukwa Collector in a safe and efficient manner.
 * Subscribe to chukwaCollector.tee.port for data streaming.
 * Defaults socket tee port is 9094.
 */
public class SocketDataLoader implements Runnable {
  private String hostname = "localhost";
  private int port = 9094;
  private static Logger log = Logger.getLogger(SocketDataLoader.class);
  private Socket s = null;
  private DataInputStream dis = null;
  private DataOutputStream dos = null;
  private Queue<Chunk> q = new LinkedList<Chunk>();
  private String recordType = null;
  private boolean running = false;
  private static final int QUEUE_MAX = 10;
  private Iterator<String> collectors = null;
  private static Pattern pattern = Pattern.compile("(.+?)\\://(.+?)\\:(.+?)");

  /*
   * Create and start an instance of SocketDataLoader.
   * @param Record Type
   */
  public SocketDataLoader(String recordType) {
    this.recordType = recordType;
    try {
      collectors = DataFactory.getInstance().getCollectorURLs(new ChukwaConfiguration());
    } catch (IOException e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    Matcher m = pattern.matcher(collectors.next());
    // Socket data loader only supports to stream data from a single collector.  
    // For large deployment, it may require to setup multi-tiers of collectors to
    // channel data into a single collector for display.
    if(m.matches()) {
      hostname = m.group(2);
    }
    start();
  }
  
  /*
   * Establish a connection to chukwa collector and filter data stream
   * base on record type.
   */
  public synchronized void start() {
    try {
      running = true;
      s = new Socket(hostname, port);
      try {
        s.setSoTimeout(120000);
        dos = new DataOutputStream (s.getOutputStream());
        StringBuilder output = new StringBuilder();
        output.append(SocketTeeWriter.WRITABLE);
        if(recordType.toLowerCase().intern()!="all".intern()) {
          output.append(" datatype=");
          output.append(recordType);
        } else {
          output.append(" all");
        }
        output.append("\n");
        dos.write((output.toString()).getBytes());
      } catch (SocketException e) {
        log.warn("Error while settin soTimeout to 120000");
      }
      dis = new DataInputStream(s
          .getInputStream());
      dis.readFully(new byte[3]); //read "OK\n"
      StringBuilder sb = new StringBuilder();
      sb.append("Subscribe to ");
      sb.append(hostname);
      sb.append(":");
      sb.append(port);
      sb.append(" for record type: ");
      sb.append(recordType);
      log.info(sb.toString());
      Thread t=new Thread (this);
      t.start();
    } catch (IOException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      stop();
    }    
  }
  
  /*
   * Read the current chunks in the SDL queue.
   * @return List of chunks in the SDL queue.
   */
  public synchronized Collection<Chunk> read() throws NoSuchElementException {
    Collection<Chunk> list = Collections.synchronizedCollection(q);
    return list;
  }
  
  /*
   * Unsubscribe from Chukwa collector and stop streaming.
   */
  public void stop() {
    if(s!=null) {
      try {
        dis.close();
        dos.close();
        s.close();
        StringBuilder sb = new StringBuilder();
        sb.append("Unsubscribe from ");
        sb.append(hostname);
        sb.append(":");
        sb.append(port);
        sb.append(" for data type: ");
        sb.append(recordType);
        log.info(sb.toString());
        running = false;
      } catch (IOException e) {
        log.debug("Unable to close Socket Tee client socket.");
      }
    }
  }

  /*
   * Check if streaming is currently happening for the current instance of SDL.
   * @return running state of the SDL,
   */
  public boolean running() {
    return running;
  }
  
  /*
   * Background thread for reading data from SocketTeeWriter, and add new data
   * into SDL queue.
   */
  @Override
  public void run() {
    try {
      Chunk c;
      while ((c = ChunkImpl.read(dis)) != null) {
        StringBuilder sb = new StringBuilder();
        sb.append("Chunk received, recordType:");
        sb.append(c.getDataType());
        log.debug(sb);
        if(q.size()>QUEUE_MAX) {
          q.poll();
        }
        q.offer(c);
      }
    } catch (IOException e) {
      log.error(ExceptionUtil.getStackTrace(e));
      stop();
    }
  }
}
