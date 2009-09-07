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

import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.sender.AsyncAckSender;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.log4j.Logger;

public class AdaptorResetThread extends Thread {
  
  Logger log = Logger.getLogger(AdaptorResetThread.class);
  
  int resetCount = 0;
  private static class AdaptorStat {
    long lastCommitTime = 0;
    long maxByteSent = 0 ;
    public AdaptorStat(long lastCommit, long maxByte) {
      maxByteSent = maxByte;
      lastCommitTime = lastCommit;
    }
  }

  Map<Adaptor, AdaptorStat> status;
  int timeout = 10*60 * 1000; //default to wait ten minutes for an ack
  ChukwaAgent agent;
  public static final String TIMEOUT_OPT = "connector.commitpoll.timeout";
  private volatile boolean running = true;
  
  public AdaptorResetThread(Configuration conf, ChukwaAgent a) {
    timeout = 2* conf.getInt(SeqFileWriter.ROTATE_INTERVAL_OPT, timeout/2);
      //default to 2x rotation interval, if rotate interval is defined.
    timeout = conf.getInt(TIMEOUT_OPT, timeout);
      //or explicitly set timeout
    status = new LinkedHashMap<Adaptor, AdaptorStat>();
    this.agent = a;
    this.setDaemon(true);
  }
  
  public void resetTimedOutAdaptors(int timeSinceLastCommit) {
    
    long timeoutThresh = System.currentTimeMillis() - timeSinceLastCommit;
    List<Adaptor> toResetList = new ArrayList<Adaptor>(); //also contains stopped 
    //adaptors
    synchronized(this) {
      for(Map.Entry<Adaptor, AdaptorStat> ent: status.entrySet()) {
        AdaptorStat stat = ent.getValue();
        ChukwaAgent.Offset off = agent.offset(ent.getKey());
        if(off == null) {
          toResetList.add(ent.getKey());
        } else if(stat.maxByteSent > off.offset 
            && stat.lastCommitTime < timeoutThresh) {
          toResetList.add(ent.getKey());
          log.warn("restarting " + off.id + " at " + off.offset + " due to collector timeout");
        }
      }
    }
    
    for(Adaptor a: toResetList) {
      status.remove(a);
      ChukwaAgent.Offset off = agent.offset(a);
      if(off != null) {
        agent.stopAdaptor(off.id, false);
        
          //We can do this safely if we're called in the same thread as the sends,
        //since then we'll be synchronous with sends, and guaranteed to be
        //interleaved between two successive sends
        //DataFactory.getInstance().getEventQueue().purgeAdaptor(a);
        
        String a_status = a.getCurrentStatus();
        agent.processAddCommand("add " + off.id + "= " + a.getClass().getCanonicalName()
             + " "+ a_status + " " + off.offset);
        resetCount ++;
        //will be implicitly added to table once adaptor starts sending
      } 
       //implicitly do nothing if adaptor was stopped
    }
  }
  
  public synchronized void reportPending(List<AsyncAckSender.CommitListEntry> delayedCommits) {
    for(AsyncAckSender.CommitListEntry dc: delayedCommits) {
      AdaptorStat a = status.get(dc.adaptor);
      if(a == null)
        status.put(dc.adaptor, new AdaptorStat(0, dc.uuid));
      else if(a.maxByteSent < dc.uuid)
          a.maxByteSent = dc.uuid;
    }
  }
  
  public void reportStop(Adaptor a) {
    status.remove(a);
  }
  
  public void run() {
    try {
      while(running) {
        Thread.sleep(timeout/2);
        resetTimedOutAdaptors(timeout);
      }
    } catch(InterruptedException e) {}
  } 
  
  public int getResetCount() {
    return resetCount;
  }
}
