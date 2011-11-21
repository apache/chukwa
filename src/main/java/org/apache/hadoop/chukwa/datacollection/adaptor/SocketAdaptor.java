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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.*;
import java.util.ArrayList;

import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggerRepository;
import org.apache.log4j.spi.LoggingEvent;

/**
 * SocketAdaptor reads TCP message from a port and convert the message to Chukwa
 * Chunk for transport from Chukwa Agent to Chukwa Collector.  Usage:
 * 
 * add SocketAdaptor [DataType] [Port] [SequenceNumber]
 * 
 */
public class SocketAdaptor extends AbstractAdaptor {
  PatternLayout layout = new PatternLayout("%d{ISO8601} %p %c: %m%n");

  private final static Logger log = Logger.getLogger(SocketAdaptor.class);
  volatile boolean running = true;
  volatile long bytesReceived = 0;
  private int port = 9095;
  
  class Dispatcher extends Thread {
    private int port;
    private ServerSocket listener;
    
    public Dispatcher(int port) {
      this.port = port;
    }
    
    public void run() {
      try{
        listener = new ServerSocket(port);
        Socket server;

        while(running){
          server = listener.accept();
          Worker connection = new Worker(server);
          Thread t = new Thread(connection);
          t.start();
        }
      } catch (IOException ioe) {
        log.error("SocketAdaptor Dispatcher problem:", ioe);
      }
    }
    
    public void shutdown() {
      try {
        listener.close();
      } catch (IOException e) {
      }
    }
  }
  
  class Worker implements Runnable {
    private ObjectInputStream ois;
    private Socket server;
    
    public Worker(Socket server) {
      this.server = server;
    }
    
    public void run() {
      LoggingEvent event;

      try {
        ois = new ObjectInputStream(
                           new BufferedInputStream(server.getInputStream()));
        if (ois != null) {
          while(running) {
            // read an event from the wire
            event = (LoggingEvent) ois.readObject();
            byte[] bytes = layout.format(event).getBytes();
            bytesReceived=bytes.length;
            Chunk c = new ChunkImpl(type, java.net.InetAddress.getLocalHost().getHostName(), bytesReceived, bytes, SocketAdaptor.this);
            dest.add(c);
          }
        }
      } catch(java.io.EOFException e) {
        log.info("Caught java.io.EOFException closing conneciton.");
      } catch(java.net.SocketException e) {
        log.info("Caught java.net.SocketException closing conneciton.");
      } catch(InterruptedIOException e) {
        Thread.currentThread().interrupt();
        log.info("Caught java.io.InterruptedIOException: "+e);
        log.info("Closing connection.");
      } catch(IOException e) {
        log.info("Caught java.io.IOException: "+e);
        log.info("Closing connection.");
      } catch(Exception e) {
        log.error("Unexpected exception. Closing conneciton.", e);
      } finally {
        if (ois != null) {
           try {
              ois.close();
           } catch(Exception e) {
              log.info("Could not close connection.", e);
           }
        }
        if (server != null) {
          try {
            server.close();
          } catch(InterruptedIOException e) {
              Thread.currentThread().interrupt();
          } catch(IOException ex) {
          }
        }
      }
    }
    
    public void shutdown() {
      try {
        ois.close();
        server.close();
      } catch (IOException e) {
      }
    }
  }
  
  Dispatcher disp;
  
  @Override
  public String parseArgs(String s) {
    port = Integer.parseInt(s);
    return s;
  }

  @Override
  public void start(long offset) throws AdaptorException {
    try {
      disp = new Dispatcher(port);
      disp.setDaemon(true);
      disp.start();      
    } catch (Exception e) {
      throw new AdaptorException(ExceptionUtil.getStackTrace(e));
    }
  }

  @Override
  public String getCurrentStatus() {
    return type + " " + port;
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    try {
      running = false;
      disp.shutdown();
    } catch(Exception e) {}
    return 0;
  }

}
