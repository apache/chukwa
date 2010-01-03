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

package org.apache.hadoop.chukwa.util;


import java.util.Random;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.*;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.chukwa.datacollection.adaptor.*;

public class MaxRateSender extends Thread implements Adaptor {

  public static final int BUFFER_SIZE = 60 * 1024;
  public static final String ADAPTOR_NAME = "MaxRateSender";

  private volatile boolean stopping = false;
  private long offset;
  private String type;
  ChunkReceiver dest;
  private String adaptorID;

  public String getCurrentStatus() {
    return type;
  }

  public void start(String adaptorID, String type, long offset,
      ChunkReceiver dest) throws AdaptorException {
    this.setName("MaxRateSender adaptor");
    this.adaptorID = adaptorID;
    this.offset = offset;
    this.type = type;
    this.dest = dest;
    super.start(); // this is a Thread.start
  }

  @Override
  public String parseArgs(String d, String s,AdaptorManager c) {
    return s;
  }

  public void run() {
    Random r = new Random();

    try {
      while (!stopping) {
        byte[] data = new byte[BUFFER_SIZE];
        r.nextBytes(data);
        offset += data.length;
        ChunkImpl evt = new ChunkImpl(type, "random data source", offset, data,
            this);
        dest.add(evt);

      }
    } catch (InterruptedException ie) {
    }
  }

  public String toString() {
    return ADAPTOR_NAME;
  }


  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy) {
    stopping = true;
    return offset;
  }
  
  @Override
  public String getType() {
    return type;
  }

}
