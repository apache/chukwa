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

import java.io.IOException;
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
  
  protected final static Logger log = Logger.getLogger(AsyncAckSender.class);
  /*
   * Represents the state required for an asynchronous ack.
   * 
   * Supplements CommitListEntry with a filename and offset;
   * the data commits when that file reaches that length.
   */
  public static class DelayedCommit extends CommitListEntry implements Comparable<DelayedCommit> {
    final String fname;
    long fOffset;
    final String aName;
    public DelayedCommit(Adaptor a, long uuid, long len, String fname, 
        long offset, String aName) {
      super(a, uuid, len);
      this.fname = fname;
      this.fOffset = offset;
      this.aName = aName;
    }

    @Override
    public int hashCode() {
      return super.hashCode() ^ fname.hashCode() ^ (int)(fOffset) ^ (int) (fOffset >> 32);
    }
    
    //sort by adaptor name first, then by start offset
    //note that returning 1 means this is "greater" than RHS
    public int compareTo(DelayedCommit o) {
      int c = o.aName.compareTo(this.aName);
      if(c != 0)
        return c;
      c = fname.compareTo(this.fname);
      if(c != 0)
        return c;
      if(o.start < start)
        return 1;
      else if(o.start > start)
        return -1;
      else return 0;
    }
    
    public String toString() {
      return adaptor +" commits from" + start + " to " + uuid + " when " + fname + " hits " + fOffset;
    }
  }
  
  public static final String POLLPERIOD_OPT = "connector.commitpoll.period";
  public static final String POLLHOSTS_OPT = "connector.commitpoll.hostfile";
  final ChukwaAgent agent;
  
  /*
   * The list of commits that we're expecting.
   * This is the structure used to pass the list to the CommitPollThread.  
   * Adjacent commits to the same file will be coalesced.
   * 
   */
  final List<DelayedCommit> mergedList;
  
  /**
   * Periodically scans a subset of the collectors, looking for committed files.
   * This way, not every collector is pestering the namenode with periodic lses.
   */
  final class CommitPollThread extends Thread {
    private ChukwaHttpSender scanPath;
    private int pollPeriod = 1000 * 30;


    private final Map<String, PriorityQueue<DelayedCommit>> pendingCommits;

    CommitPollThread(Configuration conf, Iterator<String> tryList) {
      pollPeriod = conf.getInt(POLLPERIOD_OPT, pollPeriod);
      scanPath = new ChukwaHttpSender(conf);
      scanPath.setCollectors(tryList);
      pendingCommits = new HashMap<String, PriorityQueue<DelayedCommit>>();
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
      synchronized(mergedList) {
        for(DelayedCommit dc:mergedList) {
          
          PriorityQueue<DelayedCommit> pendList = pendingCommits.get(dc.fname);
          if(pendList == null) {
            pendList = new PriorityQueue<DelayedCommit>();
            pendingCommits.put(dc.fname, pendList);
          }
          pendList.add(dc);
        }
        mergedList.clear();
      } //end synchronized
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
       
        HashSet<Adaptor> committed = new HashSet<Adaptor>();
        while(!delayedOnFile.isEmpty()) {
          DelayedCommit fired = delayedOnFile.element();
          if(fired.fOffset > committedOffset)
            break;
          else {
            ChukwaAgent.Offset o = agent.offset(fired.adaptor);
            if(o != null && fired.start > o.offset()) {
              log.error("can't commit "+ o.adaptorID() +  "  without ordering assumption");
              break; //don't commit
            }
            delayedOnFile.remove();
            String s = agent.reportCommit(fired.adaptor, fired.uuid);
            committed.add(fired.adaptor);
            //TODO: if s == null, then the adaptor has been stopped.
            //should we stop sending acks?
            log.info("COMMIT to "+ committedOffset+ " on "+ path+ ", updating " +s);
          }
        }
        adaptorReset.reportCommits(committed);
      }
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
    
    mergedList = new ArrayList<DelayedCommit>();
    this.conf = conf;
    adaptorReset = new AdaptorResetThread(conf, a);
    adaptorReset.start();
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

   pollThread = new CommitPollThread(conf, tryList);
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
    Collections.sort(delayed);
    
    synchronized(mergedList) {
      DelayedCommit region =null;
      for(DelayedCommit cur: delayed) {
        if(region == null)
          region = cur;
        else if((cur.adaptor == region.adaptor) &&
            cur.fname.equals(region.fname) && (cur.start <= region.uuid)) {
          //since the list is sorted, region.start < cur.start
          region.uuid = Math.max(region.uuid, cur.uuid); //merge
          region.fOffset = Math.max(region.fOffset, cur.fOffset);
        } else {
          mergedList.add(region);
          region= cur;
        }
      }
      mergedList.add(region);
    }
  }
  
  
  Pattern partialCommitPat = Pattern.compile("(.*) ([0-9]+)");
  @Override
  public List<CommitListEntry> postAndParseResponse(PostMethod method, 
      List<CommitListEntry> expectedCommitResults)
  throws IOException, InterruptedException {
    adaptorReset.reportPending(expectedCommitResults);
    List<String> resp = reliablySend(method, ServletCollector.PATH);
      //expect most of 'em to be delayed
    List<DelayedCommit> toDelay = new ArrayList<DelayedCommit>(resp.size());
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
          log.info("waiting for " + m.group(1) + " to hit " + m.group(2) + 
              " before committing "+ agent.getAdaptorName(cle.adaptor));
        
        String name = agent.getAdaptorName(cle.adaptor);
        if(name != null)//null name implies adaptor no longer running
          toDelay.add(new DelayedCommit(cle.adaptor, cle.uuid, cle.start, m.group(1), 
                Long.parseLong(m.group(2)), name));
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
