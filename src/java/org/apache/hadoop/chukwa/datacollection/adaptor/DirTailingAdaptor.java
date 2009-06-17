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

import java.util.regex.*;
import java.io.File;
import java.io.IOException;
import org.apache.log4j.Logger;

/**
 *  Explore a whole directory hierarchy, looking for files to tail. 
 *   DirTailingAdaptor will not try to start tailing a file more than once,
 *   if the file hasn't been modified in the interim.  
 *   
 *  Offset param is used to track last finished scan.
 *  
 *  Mandatory first parameter is a directory.  Mandatory second parameter
 *  is the name of an adaptor to start.  
 *
 */
public class DirTailingAdaptor extends AbstractAdaptor implements Runnable {
  
  static Logger log = Logger.getLogger(DirTailingAdaptor.class); 
  
  Thread scanThread = new Thread(this);
  long lastSweepStartTime;
  volatile boolean continueScanning=true;
  File baseDir;
  long scanInterval;
  String adaptorName;
  
  

  static Pattern cmd = Pattern.compile("(.+)\\s+(\\S+)");
  @Override
  public void start(String status, long offset) throws AdaptorException {
    scanInterval = control.getConfiguration().getInt("adaptor.dirscan.intervalMs", 10000);
    Matcher m = cmd.matcher(status);
    if(!m.matches() )
      throw new AdaptorException("bad syntax for DirTailer");
    else if (m.groupCount() < 2)
      throw new AdaptorException("bad syntax for DirTailer");
    baseDir = new File(m.group(1));
    adaptorName = m.group(2);
    
    scanThread.start();
    lastSweepStartTime = offset;
  }
  
  public void run() {
    try {
      while(continueScanning) {
        try {
          long sweepStartTime = System.currentTimeMillis();
          scanDirHierarchy(baseDir);
          lastSweepStartTime=sweepStartTime;
          control.reportCommit(this, lastSweepStartTime);
          Thread.sleep(scanInterval);
        } catch(IOException e) {
          log.warn(e);
        }
      }
    } catch(InterruptedException e) {
    }
  }
  
  /*
   * Coded recursively.  Base case is a single non-dir file.
   */
  private void scanDirHierarchy(File dir) throws IOException {
    if(!dir.isDirectory() ) {
      //Don't start tailing if we would have gotten it on the last pass 
      if(dir.lastModified() >= lastSweepStartTime) {
        control.processAddCommand(
            "add " + adaptorName +" " + type + " " + dir.getCanonicalPath() + " 0");
      }
    } else {
      for(File f: dir.listFiles()) {
        scanDirHierarchy(f);
      }
    }
  }

  @Override
  public String getCurrentStatus() throws AdaptorException {
    try {
      return type + " " + baseDir.getCanonicalPath()+ " " + adaptorName+ " " + lastSweepStartTime;
    } catch(IOException e) {
      throw new AdaptorException(e);
    }
  }

  @Override
  public String getStreamName() {
    return "dir scan of " + baseDir;
  }


  @Deprecated
  public long shutdown() throws AdaptorException {
    return shutdown(AdaptorShutdownPolicy.GRACEFULLY);
  }

  @Deprecated
  public void hardStop() throws AdaptorException {
    shutdown(AdaptorShutdownPolicy.HARD_STOP);
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    continueScanning = false;
    
    return lastSweepStartTime;
  }

}
