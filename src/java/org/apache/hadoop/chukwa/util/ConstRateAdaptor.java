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
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.datacollection.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.*;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.chukwa.datacollection.agent.AdaptorManager;
import org.apache.hadoop.conf.Configuration;

public class ConstRateAdaptor extends Thread implements Adaptor {

  private int SLEEP_VARIANCE = 200;
  private int MIN_SLEEP = 300;

  private String type;
  private long offset;
  private int bytesPerSec;
  private ChunkReceiver dest;
  private String adaptorID;

  private volatile boolean stopping = false;

  public String getCurrentStatus() throws AdaptorException {
    return type.trim() + " " + bytesPerSec;
  }

  public void start(String adaptorID, String type, 
      long offset, ChunkReceiver dest, AdaptorManager c) throws AdaptorException {

    this.adaptorID = adaptorID;
    this.offset = offset;
    this.type = type;
    this.dest = dest;
    this.setName("ConstRate Adaptor_" + type);
    Configuration conf = c.getConfiguration();
    MIN_SLEEP = conf.getInt("constAdaptor.minSleep", MIN_SLEEP);
    SLEEP_VARIANCE = conf.getInt("constAdaptor.sleepVariance", SLEEP_VARIANCE);
    super.start(); // this is a Thread.start
  }

  public String parseArgs(String bytesPerSecParam) {
    try {
      bytesPerSec = Integer.parseInt(bytesPerSecParam.trim());
    } catch (NumberFormatException e) {
      //("bad argument to const rate adaptor: ["  + bytesPerSecParam + "]");
      return null;
    }
    return bytesPerSecParam;
  }

  public void run() {
    Random timeCoin = new Random();
    try {
      while (!stopping) {
        int MSToSleep = timeCoin.nextInt(SLEEP_VARIANCE) + MIN_SLEEP; // between 1 and
                                                               // 3 secs
        // FIXME: I think there's still a risk of integer overflow here
        int arraySize = (int) (MSToSleep * (long) bytesPerSec / 1000L);
        byte[] data = new byte[arraySize];
        Random dataPattern = new Random(offset);
        offset += data.length;
        dataPattern.nextBytes(data);
        ChunkImpl evt = new ChunkImpl(type, "random data source", offset, data,
            this);

        dest.add(evt);

        Thread.sleep(MSToSleep);
      } // end while
    } catch (InterruptedException ie) {
    } // abort silently
  }

  public String toString() {
    return "const rate " + type;
  }

  @Deprecated
  public void hardStop() throws AdaptorException {
    shutdown(AdaptorShutdownPolicy.HARD_STOP);
  }

  @Deprecated
  public long shutdown() throws AdaptorException {
    return shutdown(AdaptorShutdownPolicy.GRACEFULLY);
  }

  @Override
  public String getType() {
    return type;
  }


  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy) {
    
    switch(shutdownPolicy) {
      case HARD_STOP :
      case GRACEFULLY : 
      case WAIT_TILL_FINISHED :
        stopping = true;
      break;
    }
    return offset;
  }
  
  public static boolean checkChunk(Chunk chunk) {
    byte[] data = chunk.getData();
    byte[] correctData = new byte[data.length];
    Random dataPattern = new Random(chunk.getSeqID());
    dataPattern.nextBytes(correctData);
    for(int i=0; i < data.length ; ++i) 
      if(data [i] != correctData[i])
        return false;
     
    return true;
  }
}
