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

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

/**
 *  Explore a whole directory hierarchy, looking for files to tail. 
 *   DirTailingAdaptor will not try to start tailing a file more than once,
 *   if the file hasn't been modified in the interim.  
 *   
 *  Offset param is used to track last finished scan.
 *  
 *  Mandatory first parameter is a directory with an optional unix style file
 *  filter. Mandatory second parameter
 *  is the name of an adaptor to start.
 * 
 *  If the specified directory does not exist, the DirTailer will continue
 *  running, and will start tailing if the directory is later created.
 *
 */
public class DirTailingAdaptor extends AbstractAdaptor implements Runnable {
  
  static Logger log = Logger.getLogger(DirTailingAdaptor.class); 
  
  Thread scanThread = new Thread(this);
  long lastSweepStartTime;
  volatile boolean continueScanning=true;
  File baseDir;
  String baseDirName; 
  long scanInterval;

	protected String adaptorName; // name of adaptors to start

  IOFileFilter fileFilter;
  String wildCharacter;

  @Override
  public void start(long offset) throws AdaptorException {
    scanInterval = control.getConfiguration().getInt("adaptor.dirscan.intervalMs", 10000);
      
    scanThread.start();
    lastSweepStartTime = offset;
    try {
      baseDirName = baseDir.getCanonicalPath();
    } catch(IOException e) {
      throw new AdaptorException(e);
    }
  }
  
  public void run() {
    try {
      log.debug("dir tailer starting to scan");
      while(continueScanning) {
        try {
          long sweepStartTime = System.currentTimeMillis();
          scanDirHierarchy(baseDir);
          lastSweepStartTime=sweepStartTime;
          control.reportCommit(this, lastSweepStartTime);
        } catch(IOException e) {
          log.warn(e);
        }
        Thread.sleep(scanInterval);
      }
    } catch(InterruptedException e) {
    }
  }
  
  /*
   * Coded recursively.  Base case is a single non-dir file.
   */
  private void scanDirHierarchy(File dir) throws IOException {
    if(!dir.exists())
      return;
    if(!dir.isDirectory()) {
      //Don't start tailing if we would have gotten it on the last pass
      if(dir.lastModified() >= lastSweepStartTime) {
				String newAdaptorID = control.processAddCommand(getAdaptorAddCommand(dir));

            log.info("DirTailingAdaptor " + adaptorID +  "  started new adaptor " + newAdaptorID);
       }
      
      } else {
        log.info("Scanning directory: " + dir.getName());
        
        for(Object f: FileUtils.listFiles(dir, fileFilter, FileFilterUtils.trueFileFilter())) {
         scanDirHierarchy((File)f);
        }
      }
  }
  
	protected String getAdaptorAddCommand(File dir) throws IOException {
		return "add " + adaptorName + " " + type + " " + dir.getCanonicalPath() + " 0";
	}

  @Override
  public String getCurrentStatus() {
    return this.wildCharacter == null ? (type + " " + baseDirName + " " + adaptorName)
    		:(type + " " + baseDirName + " " + this.wildCharacter + " " + adaptorName);
  }

  @Override
  public String parseArgs(String status) {
    
    String[] args = status.split(" ");
     
    if(args.length == 2){
     baseDir = new File(args[0]);
     fileFilter = FileFilterUtils.trueFileFilter();
     adaptorName = args[1];
    }else if(args.length == 3){
     baseDir = new File(args[0]);
     this.wildCharacter = args[ 1 ];
     fileFilter = getFileFilter();
     adaptorName = args[2]; 
    }else{
     log.warn("bad syntax in DirTailingAdaptor args");
     return null;
    }
    
    return (args.length == 2)? baseDir + " " + adaptorName : baseDir + " " + this.wildCharacter + " " + adaptorName;  //both params mandatory
  }

  @Override
  public long shutdown(AdaptorShutdownPolicy shutdownPolicy)
      throws AdaptorException {
    continueScanning = false;
    return lastSweepStartTime;
  }

  /**
   * Returns {@link IOFileFilter} constructed using the wild character. Subclasses can override this method 
   * return their own version of {@link IOFileFilter} instance.
   *  
   * @return {@link IOFileFilter} constructed using the wild character. Subclasses can override this method 
   * return their own version of {@link IOFileFilter} instance.
   */
  protected IOFileFilter getFileFilter() {
  	return new WildcardFileFilter( this.wildCharacter );
  }
}
