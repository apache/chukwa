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

package org.apache.hadoop.chukwa.datacollection.writer.localfs;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.chukwa.util.CopySequenceFile;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;


/**
 * This class is used by LocalWriter.java.
 * 
 * The only role of this class is to move dataSink files
 * from the local file system to the remote HDFS.
 * 
 * Those 2 classes are using a blockingQueue to exchange 
 * information.
 * 
 * This class will also take care of moving all existing 
 * done dataSink files (.done) and any dataSink file that
 * has not been changed for at least (rotatePeriod+2min).
 * 
 */
public class LocalToRemoteHdfsMover extends Thread {
  static Logger log = Logger.getLogger(LocalToRemoteHdfsMover.class);

  private FileSystem remoteFs = null;
  private FileSystem localFs = null;
  private Configuration conf = null;
  private String fsname = null;
  private String localOutputDir = null;
  private String remoteOutputDir = null;
  private boolean exitIfHDFSNotavailable = false;
  private BlockingQueue<String> fileQueue = null;
  private volatile boolean isRunning = true;
  
  public LocalToRemoteHdfsMover(BlockingQueue<String> fileQueue ,Configuration conf) {
    this.fileQueue = fileQueue;
    this.conf = conf;
    this.setDaemon(true);
    this.setName("LocalToRemoteHdfsMover");
    this.start();
  }

  protected void init() throws Throwable {

    // check if they've told us the file system to use
    fsname = conf.get("writer.hdfs.filesystem");
    if (fsname == null || fsname.equals("")) {
      // otherwise try to get the filesystem from hadoop
      fsname = conf.get("fs.default.name");
    }

    if (fsname == null) {
      log.error("no filesystem name");
      throw new RuntimeException("no filesystem");
    }

    log.info("remote fs name is " + fsname);
    exitIfHDFSNotavailable = conf.getBoolean(
        "localToRemoteHdfsMover.exitIfHDFSNotavailable", false);

    remoteFs = FileSystem.get(new URI(fsname), conf);
    if (remoteFs == null && exitIfHDFSNotavailable) {
      log.error("can't connect to HDFS at " + remoteFs.getUri() + " bail out!");
      DaemonWatcher.bailout(-1);
    } 
    
    localFs = FileSystem.getLocal(conf);
    
    remoteOutputDir = conf.get("chukwaCollector.outputDir", "/chukwa/logs/");
    if (!remoteOutputDir.endsWith("/")) {
      remoteOutputDir += "/";
    }
    
    localOutputDir = conf.get("chukwaCollector.localOutputDir",
    "/chukwa/datasink/");
    if (!localOutputDir.endsWith("/")) {
      localOutputDir += "/";
    }
    
  }

  protected void moveFile(String filePath) throws Exception{
    String remoteFilePath = filePath.substring(filePath.lastIndexOf("/")+1,filePath.lastIndexOf("."));
    remoteFilePath = remoteOutputDir + remoteFilePath;
    try {
      Path pLocalPath = new Path(filePath);
      Path pRemoteFilePath = new Path(remoteFilePath + ".chukwa");
      remoteFs.copyFromLocalFile(false, true, pLocalPath, pRemoteFilePath);
      Path pFinalRemoteFilePath = new Path(remoteFilePath + ".done");
      if ( remoteFs.rename(pRemoteFilePath, pFinalRemoteFilePath)) {
        localFs.delete(pLocalPath,false);
        log.info("move done deleting from local: " + pLocalPath);
      } else {
        throw new RuntimeException("Cannot rename remote file, " + pRemoteFilePath + " to " + pFinalRemoteFilePath);
      }
    }catch(FileNotFoundException ex) {
      //do nothing since if the file is no longer there it's
      // because it has already been moved over by the cleanup task.
    }
    catch (Exception e) {
      log.warn("Cannot copy to the remote HDFS",e);
      throw e;
    }
  }
  
  protected void cleanup() throws Exception{
    try {
      int rotateInterval = conf.getInt("chukwaCollector.rotateInterval",
          1000 * 60 * 5);// defaults to 5 minutes
      
      Path pLocalOutputDir = new Path(localOutputDir);
      FileStatus[] files = localFs.listStatus(pLocalOutputDir);
      String fileName = null;
      for (FileStatus file: files) {
        fileName = file.getPath().getName();
       
        if (fileName.endsWith(".recover")) {
          //.recover files indicate a previously failed copying attempt of .chukwa files
        	
          Path recoverPath= new Path(localOutputDir+fileName);
          localFs.delete(recoverPath, false);
          log.info("Deleted .recover file, " + localOutputDir + fileName);
        } else if (fileName.endsWith(".recoverDone")) {
            //.recoverDone files are valid sink files that have not been renamed to .done
        	// First, check if there are still any .chukwa files with the same name
         	
            String chukwaFileName= fileName.replace(".recoverDone", ".chukwa");
        	Boolean fileNotFound=true;
        	int i=0;
        	while (i<files.length && fileNotFound) {
        	   String currentFileName = files[i].getPath().getName();
        	   
        	   if (currentFileName.equals(chukwaFileName)){
        	      //Remove the .chukwa file found as it has already been recovered
        	      
        	     fileNotFound = false;
        	     Path chukwaFilePath = new Path(localOutputDir+chukwaFileName);
        	     localFs.delete(chukwaFilePath,false);	
        	     log.info(".recoverDone file exists, deleted duplicate .chukwa file, "
        	    		 + localOutputDir + fileName);
        	   }
        	   i++;
        	}
        	 //Finally, rename .recoverDone file to .done
        	 
        	String doneFileName= fileName.replace(".recoverDone", ".done");
        	Path donePath= new Path(localOutputDir+doneFileName);
        	Path recoverDonePath= new Path(localOutputDir+fileName);
        	localFs.rename(recoverDonePath, donePath);
        	log.info("Renamed .recoverDone file to .done, "+ localOutputDir + fileName);
         }  else if (fileName.endsWith(".done")) {
              moveFile(localOutputDir + fileName);
            }  else if (fileName.endsWith(".chukwa")) {
                 long lastPeriod = System.currentTimeMillis() - rotateInterval - (2*60*1000);
                 if (file.getModificationTime() < lastPeriod) { 
        	       //. chukwa file has not modified for some time, may indicate collector had previously crashed
         
                   log.info("Copying .chukwa file to valid sink file before moving, " + localOutputDir + fileName);
                   CopySequenceFile.createValidSequenceFile(conf,localOutputDir,fileName,localFs);
                  }
               } 
        }
    } catch (Exception e) {
        log.warn("Cannot copy to the remote HDFS",e);
        throw e;
      }
  }
  
  @Override
  public void run() {
    boolean inError = true;
    String filePath = null;
    
    while (isRunning) {
      try {
        if (inError) {
          init();
          cleanup();
          inError = false;
        }

        filePath = fileQueue.take();
        
        if (filePath == null) {
          continue;
        }
        
        moveFile(filePath);
        cleanup();
        filePath = null;
        
      } catch (Throwable e) {
        log.warn("Error in LocalToHdfsMover", e);
        inError = true;
        try {
          log.info("Got an exception going to sleep for 60 secs");
          Thread.sleep(60000);
        } catch (Throwable e2) {
          log.warn("Exception while sleeping", e2);
        }
      }
    }
    log.info(Thread.currentThread().getName() + " is exiting.");
  }

  public void shutdown() {
    this.isRunning = false;
  }
}
