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
package org.apache.hadoop.chukwa.datacollection.collector.servlet;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.log4j.Logger;
import java.util.*;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.chukwa.extraction.archive.SinkArchiver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class CommitCheckServlet extends HttpServlet {

  private static final long serialVersionUID = -4627538252371890849L;
  
  protected static Logger log = Logger.getLogger(CommitCheckServlet.class);
  CommitCheckThread commitCheck;
  Configuration conf;
    //interval at which to scan the filesystem, ms
  public static final String SCANPERIOD_OPT = "chukwaCollector.asyncAcks.scanperiod";
  
    //interval at which to discard seen files, ms
  public static final String PURGEDELAY_OPT = "chukwaCollector.asyncAcks.purgedelay"; 
    
  //list of dirs to search, separated by commas
  public static final String SCANPATHS_OPT = "chukwaCollector.asyncAcks.scanpaths";
    
  public static final String DEFAULT_PATH = "acks"; //path to this servlet on collector
  public CommitCheckServlet(Configuration conf) {
    this.conf = conf;
  }
  
  public void init(ServletConfig servletConf) throws ServletException {
    log.info("initing commit check servlet");
    try {
      FileSystem fs = FileSystem.get(
          new URI(conf.get("writer.hdfs.filesystem", "file:///")), conf);
      log.info("commitcheck fs is " + fs.getUri());
      commitCheck = new CommitCheckThread(conf, fs);
      commitCheck.start();
    } catch(Exception e) {
      log.error("couldn't start CommitCheckServlet", e);
      throw new ServletException(e);
    }
  }

  @Override
  protected void doTrace(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException { 
    resp.sendError(HttpServletResponse.SC_METHOD_NOT_ALLOWED); 
  }
  
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException  {
  
    PrintStream out = new PrintStream(resp.getOutputStream());
    resp.setStatus(200);

    out.println("<html><body><h2>Commit status</h2><ul>");
    for(String s: commitCheck.getLengthList()) 
      out.println("<li>" + s + "</li>");
    out.println("</ul></body></html>");
  }
  

  @Override
  public void destroy() {
    commitCheck.shutdown();
  }
  
  /**
   * Ideally, we'd use zookeeper to monitor archiver/demux rotation.
   * For now, instead, we'll just do an ls in a bunch of places.
   */
  private static class CommitCheckThread extends Thread implements CHUKWA_CONSTANT {
    int checkInterval = 1000 * 30;
    volatile boolean running = true;
    final Collection<Path> pathsToSearch;
    final FileSystem fs;
    final Map<String, Long> lengthTable;
    final PriorityQueue<PurgeTask> oldEntries;
    long delayUntilPurge = 1000 * 60 * 60 * 12;
    
    static class PurgeTask implements Comparable<PurgeTask>{
      long purgeTime;
      String toPurge;
      long len;
      
      public PurgeTask(String s, long time, long len) {
        this.toPurge = s;
        this.purgeTime = time;
        this.len = len;
      }
      
      public int compareTo(PurgeTask p) {
        if(purgeTime < p.purgeTime)
          return -1;
        else if (purgeTime == p.purgeTime)
          return 0;
        else
          return 1;
      }
    }
    
    
    public CommitCheckThread(Configuration conf, FileSystem fs) {
      this.fs = fs;
      pathsToSearch = new ArrayList<Path>();
      lengthTable = new LinkedHashMap<String, Long>();
      oldEntries = new PriorityQueue<PurgeTask>();
      checkInterval = conf.getInt(SCANPERIOD_OPT, checkInterval);
      
      String sinkPath = conf.get(SeqFileWriter.OUTPUT_DIR_OPT, "/chukwa/logs");
      pathsToSearch.add(new Path(sinkPath));
      
      String additionalSearchPaths = conf.get(SCANPATHS_OPT, "");
      String[] paths = additionalSearchPaths.split(",");
      for(String s: paths)
        if(s.length() > 1) {
          Path path = new Path(s);
          if(!pathsToSearch.contains(path))
            pathsToSearch.add(path);
        }
      
      delayUntilPurge = conf.getLong(PURGEDELAY_OPT, delayUntilPurge);
      String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD, DEFAULT_CHUKWA_ROOT_DIR_NAME);
      String archivesRootProcessingDir = chukwaRootDir + ARCHIVES_PROCESSING_DIR_NAME;
      String archivesMRInputDir = archivesRootProcessingDir + ARCHIVES_MR_INPUT_DIR_NAME;
      pathsToSearch.add(new Path(archivesMRInputDir));
      //TODO: set checkInterval using conf
    }
    
    public void shutdown() {
      running = false;
      this.interrupt();
    }
    
    public void run() {
      while(running) {
        try {
          Thread.sleep(checkInterval);
          scanFS();
          purgeOldEntries();
        } catch(InterruptedException e) {}
          catch(IOException e) {
           log.error("io problem", e);
        }
      }
   }

    private synchronized void purgeOldEntries() {
      long now = System.currentTimeMillis();
      PurgeTask p = oldEntries.peek();
      while(p != null && p.purgeTime < now) {
        oldEntries.remove();
        Long curLen = lengthTable.get(p.toPurge);
        if(curLen != null && p.len >= curLen)
          lengthTable.remove(p.toPurge);
      }
      
    }

    private void scanFS() throws IOException {
      long nextPurgeTime = System.currentTimeMillis() + delayUntilPurge;
      for(Path dir: pathsToSearch) {
        int filesSeen = 0;
        
        FileStatus[] dataSinkFiles = fs.listStatus(dir, SinkArchiver.DATA_SINK_FILTER);
        if(dataSinkFiles == null || dataSinkFiles.length == 0)
          continue;
        
        synchronized(this) {
          for(FileStatus fstatus: dataSinkFiles) {
            filesSeen++;
            String name = fstatus.getPath().getName();
            long len = fstatus.getLen();
            oldEntries.add(new PurgeTask(name, nextPurgeTime, len));
            lengthTable.put(name, len);
          }
        }
        log.info("scanning fs: " + dir + "; saw "+ filesSeen+ " files");
      }
    }

    public synchronized List<String> getLengthList() {
      ArrayList<String> list = new ArrayList<String>(lengthTable.size());
      for(Map.Entry<String, Long> e: lengthTable.entrySet()) {
        list.add(e.getKey() + " " + e.getValue());
      }
      return list;
    }
    
  }

}
