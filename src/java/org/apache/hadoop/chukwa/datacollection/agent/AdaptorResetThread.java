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
import org.apache.hadoop.chukwa.datacollection.adaptor.AdaptorShutdownPolicy;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.CommitCheckServlet;
import org.apache.hadoop.chukwa.datacollection.sender.AsyncAckSender;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.log4j.Logger;

public class AdaptorResetThread extends Thread {
  
  static Logger log = Logger.getLogger(AdaptorResetThread.class);
  public static final String TIMEOUT_OPT = "connector.commitpoll.timeout";
  
  
  int resetCount = 0;
  private static class AdaptorStat {
    long lastCommitTime = 0;
    long maxByteSent = 0 ;
    public AdaptorStat(long lastCommit, long maxByte) {
      maxByteSent = maxByte;
      lastCommitTime = lastCommit;
    }
  }


  int timeout = 15*60 * 1000; //default to wait fifteen minutes for an ack
               //note that this is overridden using the poll and rotate periods.
  
  Map<Adaptor, AdaptorStat> status;
  ChukwaAgent agent;
  private volatile boolean running = true;
  
  public AdaptorResetThread(Configuration conf, ChukwaAgent a) {
        //
    timeout =  conf.getInt(SeqFileWriter.ROTATE_INTERVAL_OPT, timeout/3) 
        + conf.getInt(AsyncAckSender.POLLPERIOD_OPT, timeout/3) 
        + conf.getInt(CommitCheckServlet.SCANPERIOD_OPT, timeout/3);
    
    timeout = conf.getInt(TIMEOUT_OPT, timeout); //unless overridden
     
    status = new LinkedHashMap<Adaptor, AdaptorStat>();
    this.agent = a;
    this.setDaemon(true);
  }
  
  /**
   * Resets all adaptors with outstanding data more than timeSinceLastCommit old.
   * @param timeSinceLastCommit
   * @return the number of reset adaptors
   */
  public int resetTimedOutAdaptors(int timeSinceLastCommit) {
    int resetThisTime = 0;
    long timeoutThresh = System.currentTimeMillis() - timeSinceLastCommit;
    List<Adaptor> toResetList = new ArrayList<Adaptor>(); //also contains stopped 
    //adaptors
    synchronized(this) {
      for(Map.Entry<Adaptor, AdaptorStat> ent: status.entrySet()) {
        AdaptorStat stat = ent.getValue();
        ChukwaAgent.Offset off = agent.offset(ent.getKey());
        if(off == null) {
          toResetList.add(ent.getKey());
        } else if(stat.maxByteSent > off.offset  //some data outstanding
            && stat.lastCommitTime < timeoutThresh) { //but no progress made
          toResetList.add(ent.getKey());
          log.warn("restarting " + off.id + " at " + off.offset + " due to timeout; "+
              "last commit was ");
        }
      }
    }
    
    for(Adaptor a: toResetList) {
      status.remove(a); //it'll get added again when adaptor resumes, if it does
      ChukwaAgent.Offset off = agent.offset(a);
      if(off != null) {
        agent.stopAdaptor(off.id, AdaptorShutdownPolicy.RESTARTING);
        
        String a_status = a.getCurrentStatus();
        agent.processAddCommand("add " + off.id + "= " + a.getClass().getCanonicalName()
             + " "+ a_status + " " + off.offset);
        resetThisTime ++;
        //will be implicitly added to table once adaptor starts sending
      } 
       //implicitly do nothing if adaptor was stopped. We already removed
      //its entry from the status table.
    }
    resetCount += resetThisTime;
    return resetThisTime;
  }
  
  public synchronized void reportPending(List<AsyncAckSender.CommitListEntry> delayedCommits) {
    long now = System.currentTimeMillis();
    for(AsyncAckSender.CommitListEntry dc: delayedCommits) {
      AdaptorStat a = status.get(dc.adaptor);
      if(a == null)
        status.put(dc.adaptor, new AdaptorStat(now, dc.uuid));
      else if(a.maxByteSent < dc.uuid)
          a.maxByteSent = dc.uuid;
    }
  }
  
  public synchronized void reportCommits(Set<Adaptor> commits) {
    long now = System.currentTimeMillis();
    for(Adaptor a: commits) {
      if(status.containsKey(a)) {
        status.get(a).lastCommitTime = now;
      } else
        log.warn("saw commit for adaptor " + a + " before seeing sends"); 
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
