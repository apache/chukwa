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
package org.apache.hadoop.chukwa.datacollection.sender;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.*;
import org.apache.hadoop.chukwa.datacollection.adaptor.Adaptor;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.CommitCheckServlet;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.sender.ChukwaHttpSender.CommitListEntry;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.*;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
//import com.google.common.collect.SortedSetMultimap;
//import com.google.common.collect.TreeMultimap;

import org.apache.log4j.Logger;

/**
 * An enhancement to ChukwaHttpSender that handles asynchronous acknowledgment.
 * 
 * This class will periodically poll the collectors to find out how much data
 * has been committed to HDFS, and will then pass those acks on to the Agent.
 */
public class AsyncAckSender extends ChukwaHttpSender{
  
  protected static Logger log = Logger.getLogger(AsyncAckSender.class);
  /*
   * Represents the state required for an asynchronous ack.
   * 
   * Supplements CommitListEntry with a filename and offset;
   * the data commits when that file reaches that length.
   */
  public static class DelayedCommit extends CommitListEntry implements Comparable<DelayedCommit> {
    final String fname;
    final long offset;
    public DelayedCommit(Adaptor a, long uuid, String fname, long offset) {
      super(a, uuid);
      this.fname = fname;
      this.offset = offset;
    }

    @Override
    public int hashCode() {
      return super.hashCode() ^ fname.hashCode() ^ (int)(offset) ^ (int) (offset >> 32);
    }
    
    public int compareTo(DelayedCommit o) {
      if(o.uuid < uuid)
        return 1;
      else if(o.uuid > uuid)
        return -1;
      else return 0;
    }
    
    public String toString() {
      return adaptor +" commits up to " + uuid + " when " + fname + " hits " + offset;
    }
  }
  
  public static final String POLLPERIOD_OPT = "connector.commitpoll.period";
  public static final String POLLHOSTS_OPT = "connector.commitpoll.hostfile";
  final ChukwaAgent agent;
  
  /*
   * The merge table stores commits that we're expecting, before they're handed
   * to the CommitPollThread.  There will be only one entry for each adaptor.
   * 
   * values are a collection of delayed commits, one per adaptor.
   * keys are unspecified
   */
  final Map<String, DelayedCommit> mergeTable;
  
  /**
   * Periodically scans a subset of the collectors, looking for committed files.
   * This way, not every collector is pestering the namenode with periodic lses.
   */
  static final class CommitPollThread extends Thread {
    private ChukwaHttpSender scanPath;
    private int pollPeriod = 1000 * 30;


    private final Map<String, PriorityQueue<DelayedCommit>> pendingCommits;
    private final Map<String, DelayedCommit> mergeTable;
    private final ChukwaAgent agent;

    CommitPollThread(Configuration conf, ChukwaAgent agent, 
        Map<String, DelayedCommit> mergeTable, Iterator<String> tryList) {
      pollPeriod = conf.getInt(POLLPERIOD_OPT, pollPeriod);
      scanPath = new ChukwaHttpSender(conf);
      scanPath.setCollectors(tryList);
      pendingCommits = new HashMap<String, PriorityQueue<DelayedCommit>>();
      this.mergeTable = mergeTable;
      this.agent = agent;
    }

    private volatile boolean running = true;
    public void shutdown() {
      running = false;
      this.interrupt();
    }
    
    public void run() {
      try {
        while(running) {
          Thread.sleep(pollPeriod);
          //update table using list of pending delayed commits, in this thread
          checkForCommits();
          mergePendingTable();
        }
      } catch(InterruptedException e) {}
      catch(IOException e) {
        log.error(e);
      }
    } 
    
    /*
     * Note that this method is NOT threadsafe, and should only be called
     * from the same thread that will later check for commits
     */
    private void mergePendingTable() {
      synchronized(mergeTable) {
        for(DelayedCommit dc: mergeTable.values()) {
          
          PriorityQueue<DelayedCommit> map = pendingCommits.get(dc.fname);
          if(map == null) {
            map = new PriorityQueue<DelayedCommit>();
            pendingCommits.put(dc.fname, map);
          }
          map.add(dc);
        }
        mergeTable.clear();
      }
    }
    
    Pattern respLine = Pattern.compile("<li>(.*) ([0-9]+)</li>");
    private void checkForCommits() throws IOException, InterruptedException {
      
      log.info("checking for commited chunks");
      GetMethod method = new GetMethod();
      List<String> parsedFStatuses = scanPath.reliablySend(method, CommitCheckServlet.DEFAULT_PATH); 

      //do an http get
      for(String stat: parsedFStatuses) {
        Matcher m = respLine.matcher(stat);
        if(!m.matches())
          continue;
        String path = m.group(1);
        Long committedOffset = Long.parseLong(m.group(2));

        PriorityQueue<DelayedCommit> delayedOnFile = pendingCommits.get(path);
        if(delayedOnFile == null)
          continue;
       
        while(!delayedOnFile.isEmpty()) {
          DelayedCommit fired = delayedOnFile.element();
          if(fired.offset > committedOffset)
            break;
          else
            delayedOnFile.remove();
          String s = agent.reportCommit(fired.adaptor, fired.uuid);
          //TODO: if s == null, then the adaptor has been stopped.
          //should we stop sending acks?
          log.info("COMMIT to "+ committedOffset+ " on "+ path+ ", updating " +s);
        }
      }
    }

    void setScannableCollectors(Iterator<String> collectorURLs) {
      // TODO Auto-generated method stub
      
    }
  } 
  
  CommitPollThread pollThread;
  
  //note that at present we don't actually run this thread; we just use its methods.
  public AdaptorResetThread adaptorReset;
  Configuration conf;
  
  public AsyncAckSender(Configuration conf, ChukwaAgent a) throws IOException {
    super(conf);
    log.info("delayed-commit processing enabled");
    agent = a;
    
    mergeTable = new LinkedHashMap<String, DelayedCommit>();
    this.conf = conf;
    adaptorReset = new AdaptorResetThread(conf, a);
    //initialize the commitpoll later, once we have the list of collectors
  }
  
  
  @Override
  public void setCollectors(Iterator<String> collectors) {
   Iterator<String> tryList = null;
   String scanHostsFilename = conf.get(POLLHOSTS_OPT, "collectors");
   try {
     tryList = DataFactory.getInstance().getCollectorURLs(conf, scanHostsFilename);
   } catch(IOException e) {
     log.warn("couldn't read " + scanHostsFilename+ " falling back on collectors list");
   }

   if(collectors instanceof RetryListOfCollectors) {
     super.setCollectors(collectors);
     if(tryList == null)
       tryList = ((RetryListOfCollectors) collectors).clone();
   } 
   else {
     ArrayList<String> l = new ArrayList<String>();
     while(collectors.hasNext())
       l.add(collectors.next());
     super.setCollectors(l.iterator());
     if(tryList == null)
       tryList = l.iterator();
   }

   pollThread = new CommitPollThread(conf, agent, mergeTable, tryList);
   pollThread.setDaemon(true);
   pollThread.start();
  }
  
  /*
   * This method is the interface from AsyncAckSender to the CommitPollThread --
   * it gets a lock on the merge table, and then updates it with a batch of pending acks
   *
   *  This method is called from the thread doing a post; the merge table is
   *  read by the CommitPollThread when it figures out what commits are expected.
   */
  private void delayCommits(List<DelayedCommit> delayed) {
    String[] keys = new String[delayed.size()];
    int i = 0;
    for(DelayedCommit c: delayed) {
      String adaptorKey = c.adaptor.hashCode() + "_" + c.adaptor.getCurrentStatus().hashCode();
      keys[i++] = c.fname +"::" + adaptorKey;
    }
    synchronized(mergeTable) {
      for(i = 0; i < keys.length; ++i) {
        DelayedCommit cand = delayed.get(i);
        DelayedCommit cur = mergeTable.get(keys[i]);
        if(cur == null || cand.offset > cur.offset) 
          mergeTable.put(keys[i], cand);
      }
    }
  }
  
  
  Pattern partialCommitPat = Pattern.compile("(.*) ([0-9]+)");
  @Override
  public List<CommitListEntry> postAndParseResponse(PostMethod method, 
      List<CommitListEntry> expectedCommitResults)
  throws IOException, InterruptedException {
    adaptorReset.reportPending(expectedCommitResults);
    List<String> resp = reliablySend(method, ServletCollector.PATH);
    List<DelayedCommit> toDelay = new ArrayList<DelayedCommit>();
    ArrayList<CommitListEntry> result =  new ArrayList<CommitListEntry>();
    for(int i = 0; i < resp.size(); ++i)  {
      if(resp.get(i).startsWith(ServletCollector.ACK_PREFIX))
        result.add(expectedCommitResults.get(i));
      else {
        CommitListEntry cle = expectedCommitResults.get(i);
        Matcher m = partialCommitPat.matcher(resp.get(i));
        if(!m.matches())
          log.warn("unexpected response: "+ resp.get(i));
        else
          log.info("waiting for " + m.group(1) + " to hit " + m.group(2) + " before committing "+ cle.adaptor);
        toDelay.add(new DelayedCommit(cle.adaptor, cle.uuid, m.group(1), 
            Long.parseLong(m.group(2))));
      }
    }
    delayCommits(toDelay);
    return result;
  }
  
  @Override
  protected boolean failedCollector(String downed) {
    log.info("collector "+ downed + " down; resetting adaptors");
    adaptorReset.resetTimedOutAdaptors(0); //reset all adaptors with outstanding data.
    return false;
  }
  
  @Override
  public void stop() {
    pollThread.shutdown();
  }

}
