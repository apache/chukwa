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
package edu.berkeley.chukwa_xtrace;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.util.*;
import org.apache.hadoop.io.Text;
import edu.berkeley.chukwa_xtrace.XtrExtract.PtrReverse;
import edu.berkeley.xtrace.reporting.Report;

/**
 * Encapsulates a causal graph; nodes are xtrace reports.
 *
 */
public class CausalGraph implements Iterable<Report> {
  
  /**
   * Returns the distance from report src to dest.
   * Should be positive if dest happened after src
   * @author asrabkin
   *
   */
  public static class RDistMetric {
    long dist(Report src, Report dest) {
      return getTS(src);
    }
  }
  
  public static class IgnoreReportsMetric extends RDistMetric {
    List<Report> pathToIgnore;
    /**
     * Path should be reverse-ordered, same as longestPath returns
     * @param pathToIgnore
     */
    public IgnoreReportsMetric(List<Report> pathToIgnore) {
      this.pathToIgnore = pathToIgnore;
    }
    
    @Override
    long dist(Report src, Report dest) {
      for(int i =0; i < pathToIgnore.size()-1; ++i) {
        if((pathToIgnore.get(i+1) == src) && (pathToIgnore.get(i) == dest))
          return 0;
      }
      return getTS(src);
    }
    
  }
  
  
  private Map<String, Report> reports;
  private Report start;
  private Report end;
  
  public CausalGraph(Map<String, Report> reports) {
    this.reports = reports;
  }
  
  public CausalGraph() {
    reports = new LinkedHashMap<String, Report>();
  }
  
  
  public Report getStart() {
    return start;
  }

  public void setStart(Report start) {
    this.start = start;
  }

  public Report getEnd() {
    return end;
  }

  public void setEnd(Report end) {
    this.end = end;
  }

  public void add(Report r) {
    String opID = r.getMetadata().getOpIdString();
    reports.put(opID, r);
  }
  
  
  ///////  Graph-analytic functions
  
  public Set<Report> predecessors(Report p) {
    
    HashSet<Report> predecessors = new HashSet<Report>();
    Queue<Report> bfsQ = new LinkedList<Report>();
    bfsQ.add(p);
    while(!bfsQ.isEmpty()) {
      Report r = bfsQ.remove();

      assert r!= null;
      predecessors.add(r);
      List<String> backEdges = r.get("Edge");
      if(backEdges != null)
        for(String pred:backEdges) {
          Report pre = reports.get(pred);
          if(pre != null)
            bfsQ.add(pre);
        }
    }
    
    return predecessors;
  }
  
  public List<Report> topoSort(PtrReverse reverser) {
    HashMap<String, Integer> counts = new HashMap<String, Integer>();
    Queue<Report> zeroInlinkReports = new LinkedList<Report>();

    //FIXME: could usefully compare reports.size() with numReports;
    //that would measure duplicate reports
    
    //increment link counts for children
    for(Report r: reports.values()){ 
      String myOpID = r.getMetadata().getOpIdString();
      
      int parentCount = reverser.setupForwardPointers(reports, r, myOpID);
        
      //if there weren't any parents, we can dequeue
      if(parentCount == 0)
        zeroInlinkReports.add(r);
      else
        counts.put(myOpID, parentCount);
    }
    
    //at this point, we have a map from metadata to report, and also
    //from report op ID to inlink count.
    //next step is to do a topological sort.

    ArrayList<Report> finalOutput = new ArrayList<Report>();
    while(!zeroInlinkReports.isEmpty()) {
      Report r = zeroInlinkReports.remove();
      
      List<String> outLinks =  r.get(XtrExtract.OUTLINK_FIELD);
      if(outLinks != null) {
        for(String outLink: outLinks) {
          Integer oldCount = counts.get(outLink);
          if(oldCount == null) {
            oldCount = 0;  //FIXME: can this happen?
              //Means we have a forward-edge to a node which we haven't ever set up a link count for
      //      log.warn(taskIDString+": found an in-edge where none was expected");
          } if(oldCount == 1) {
            zeroInlinkReports.add(reports.get(outLink));
          }
          counts.put(outLink, oldCount -1);
        }
      }
    }
    return finalOutput;
  }
  
  
  
  ///////  Performance-analytic functions
  
  private static final long getTS(Report r) {
    List<String> staTL = r.get("Timestamp");
    if(staTL != null && staTL.size() > 0) {

      double t = Double.parseDouble(staTL.get(0));
      return Math.round(1000 * t);
    }
    return Long.MIN_VALUE;      
  }
  
  /**
   * Returns the longest path ending at endID
   * 
   * Path is in reversed order, starting with endID and going forwards.
   * 
   * @param endID
   * @return
   */
  
  public List<Report> longestPath(String endID) {
    return longestPath(new RDistMetric(), endID);
  }

  public List<Report> longestPath(RDistMetric metric, String endID) {
    //if we have the reports in topological order, this should be easy.
    //Just take max of all predecessors seen until that point.
    
    //alternatively, could start at the end and walk backwards
    ArrayList<Report> backpath = new ArrayList<Report>();
    Report cur = reports.get(endID);
    do {
      backpath.add(cur);
      
      Report limitingPred = null;
      long latestPrereq = Long.MIN_VALUE;
      
      for(String predID: cur.get("Edge")) {
        Report pred = reports.get(predID);
        long finishTime = metric.dist(pred, cur);
        if( finishTime > latestPrereq) {
          latestPrereq = finishTime;
          limitingPred = pred;
        }
        cur = limitingPred;  
      }
    } while(cur != null && cur.get("Edge") != null);
    
    //should be able to just walk forward, keeping trac
    return backpath;
  }
  
  /**
   * Expect path to be sorted backwards.
   * @param path
   * @return
   */
  public static long onHostTimes(List<Report> path) {
    long time =0;   
    for(int i =0; i < path.size()-1; ++i) {
      Report src = path.get(i+1);
      Report dest = path.get(i);
      List<String> srcHost = src.get("Host"), destHost = dest.get("Host");
      if(srcHost != null && srcHost.size() > 0 && destHost != null && destHost.size() > 0) {
        if(srcHost.get(0).equals(destHost.get(0))){
          long src_ts = getTS(src);
          long dest_ts = getTS(dest);
          
          time += (dest_ts - src_ts);
          System.out.println("adding segment of length " + (dest_ts - src_ts));
        }
      }
    }
    return time;
      
  }
  
  
  ////  Glue to make CausalGraph look like a pseudocollection
  public Iterator<Report> iterator() {
    return reports.values().iterator();
  }
  
  public int size() {
    return reports.size();
  }
  
  public Collection<Report> getReports() {
    return reports.values();
  }
  
  //////IO utils
  
  public void slurpTextFile(File f) throws IOException {
    
    BufferedReader br = new BufferedReader(new FileReader(f));
    Report rep;
    while((rep = getNextReportFromReader(br)) != null ) {
      add(rep);
    }
  }
  
  private Report getNextReportFromReader(BufferedReader br) throws IOException {
    StringBuilder sb = new StringBuilder();
    
    String s;
    while((s = br.readLine()) != null ) {
      if(s.length() > 1) {
        sb.append(s);
        sb.append("\n");
      } else    //stop on blank line, if it isn't the first one we see
        if(sb.length() > 1)
          break;
    }
    if(sb.length() < 1)
      return null;
    
    return Report.createFromString(sb.toString());
  }
  
}
