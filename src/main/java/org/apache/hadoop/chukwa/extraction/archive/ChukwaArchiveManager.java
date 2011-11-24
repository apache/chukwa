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

package org.apache.hadoop.chukwa.extraction.archive;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ChukwaArchiveManager implements CHUKWA_CONSTANT {
  static Logger log = Logger.getLogger(ChukwaArchiveManager.class);
  static SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd");
  
  static final  int ONE_HOUR = 60 * 60 * 1000;
  static final int ONE_DAY = 24*ONE_HOUR;
  static final int MAX_FILES = 500;

  private static final int DEFAULT_MAX_ERROR_COUNT = 4;

  protected ChukwaConfiguration conf = null;
  protected FileSystem fs = null;
  protected boolean isRunning = true;
  
  public ChukwaArchiveManager() throws Exception { 
    conf = new ChukwaConfiguration();
    init();
  }

  public ChukwaArchiveManager(ChukwaConfiguration conf) throws Exception {
    conf = new ChukwaConfiguration();
    init();
  }
  
  protected void init() throws IOException, URISyntaxException {
    String fsName = conf.get(HDFS_DEFAULT_NAME_FIELD);
    fs = FileSystem.get(new URI(fsName), conf);
  }

  public static void main(String[] args) throws Exception {
    DaemonWatcher.createInstance("ArchiveManager");
    
    ChukwaArchiveManager manager = new ChukwaArchiveManager();
    manager.start();
  }

  public void shutdown() {
    this.isRunning = false;
  }
  
  public void start() throws Exception {
    
    String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD, DEFAULT_CHUKWA_ROOT_DIR_NAME);
    if ( ! chukwaRootDir.endsWith("/") ) {
      chukwaRootDir += "/";
    }
    log.info("chukwaRootDir:" + chukwaRootDir);
    
    String archiveRootDir = conf.get(CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +DEFAULT_CHUKWA_DATASINK_DIR_NAME);
    if ( ! archiveRootDir.endsWith("/") ) {
      archiveRootDir += "/";
    }
    log.info("archiveDir:" + archiveRootDir);
    Path pArchiveRootDir = new Path(archiveRootDir);
    setup(pArchiveRootDir);
    
    String archivesRootProcessingDir = chukwaRootDir + ARCHIVES_PROCESSING_DIR_NAME;
    // String archivesErrorDir = archivesRootProcessingDir + DEFAULT_ARCHIVES_IN_ERROR_DIR_NAME;
    String archivesMRInputDir = archivesRootProcessingDir + ARCHIVES_MR_INPUT_DIR_NAME;
    String archivesMROutputDir = archivesRootProcessingDir+ ARCHIVES_MR_OUTPUT_DIR_NAME;
    String finalArchiveOutput = chukwaRootDir + DEFAULT_FINAL_ARCHIVES;

    int maxPermittedErrorCount = conf.getInt(CHUKWA_ARCHIVE_MAX_ERROR_COUNT_FIELD,
                                             DEFAULT_MAX_ERROR_COUNT);
    
    Path pDailyRawArchivesInput = new Path(archiveRootDir);
    Path pArchivesMRInputDir = new Path(archivesMRInputDir);
    Path pArchivesRootProcessingDir = new Path(archivesRootProcessingDir);
    Path pFinalArchiveOutput =  new Path(finalArchiveOutput);
    
    
    if (!archivesMRInputDir.endsWith("/")) {
      archivesMRInputDir +="/";
    }
    setup( pArchivesRootProcessingDir );
    setup( pDailyRawArchivesInput );
    setup( pFinalArchiveOutput );
    
    int errorCount = 0;
    
    long lastRun = 0l;
    
    while (isRunning) {
      try {
        
        if (maxPermittedErrorCount != -1 && errorCount >= maxPermittedErrorCount) {
          log.warn("==================\nToo many errors (" + errorCount +
                   "), Bail out!\n==================");
          DaemonWatcher.bailout(-1);
        }
        // /chukwa/archives/<YYYYMMDD>/dataSinkDirXXX
        //  to
        // /chukwa/archives/final/<YYYYMMDD>_<TS>
        
        if (fs.exists(pArchivesMRInputDir)) {
          FileStatus[] days = fs.listStatus(pArchivesMRInputDir);
          if (days.length > 0) {
            log.info("reprocessing current Archive input" +  days[0].getPath());
            
            runArchive(archivesMRInputDir + days[0].getPath().getName() + "/",archivesMROutputDir,finalArchiveOutput);  
            errorCount = 0;
            continue;
          }
        }
        
        
        log.info("Raw Archive dir:" + pDailyRawArchivesInput);
        long now = System.currentTimeMillis();
        int currentDay = Integer.parseInt(day.format(System.currentTimeMillis()));
        FileStatus[] daysInRawArchiveDir = fs.listStatus(pDailyRawArchivesInput);
        
        if (daysInRawArchiveDir.length == 0 ) { 
          log.debug( pDailyRawArchivesInput + " is empty, going to sleep for 1 minute"); 
          Thread.sleep(1 * 60 * 1000); 
          continue; 
        } 
        // We don't want to process DataSink file more than once every 2 hours
        // for current day
        if (daysInRawArchiveDir.length == 1 ) {
          int workingDay = Integer.parseInt(daysInRawArchiveDir[0].getPath().getName());
          long nextRun = lastRun + (2*ONE_HOUR) - (1*60*1000);// 2h -1min
          if (workingDay == currentDay && now < nextRun) {
            log.info("lastRun < 2 hours so skip archive for now, going to sleep for 30 minutes, currentDate is:" + new java.util.Date());
            Thread.sleep(30 * 60 * 1000);
            continue;
          }
        }
        
        String dayArchivesMRInputDir = null;
        for (FileStatus fsDay : daysInRawArchiveDir) {
          dayArchivesMRInputDir = archivesMRInputDir + fsDay.getPath().getName() + "/";
          processDay(fsDay, dayArchivesMRInputDir,archivesMROutputDir, finalArchiveOutput);
          lastRun = now;
        }
        
      }catch (Throwable e) {
        errorCount ++;
        e.printStackTrace();
        log.warn(e);
      }
      
    }
    
  }
  
  public void processDay(FileStatus fsDay, String archivesMRInputDir,
      String archivesMROutputDir,String finalArchiveOutput) throws Exception {
    FileStatus[] dataSinkDirsInRawArchiveDir = fs.listStatus(fsDay.getPath());
    long now = System.currentTimeMillis();
    
    int currentDay = Integer.parseInt(day.format(System.currentTimeMillis()));
    int workingDay = Integer.parseInt(fsDay.getPath().getName());
    
    long oneHourAgo = now -  ONE_HOUR;
    if (dataSinkDirsInRawArchiveDir.length == 0 && workingDay < currentDay) {
      fs.delete(fsDay.getPath(),false);
      log.info("deleting raw dataSink dir for day:" + fsDay.getPath().getName());
      return;
    }
    
    int fileCount = 0;
    for (FileStatus fsDataSinkDir : dataSinkDirsInRawArchiveDir) {
      long modificationDate = fsDataSinkDir.getModificationTime();
      if (modificationDate < oneHourAgo || workingDay < currentDay) {
        log.info("processDay,modificationDate:" + modificationDate +", adding: " + fsDataSinkDir.getPath() );
        fileCount += fs.listStatus(fsDataSinkDir.getPath()).length;
        moveDataSinkFilesToArchiveMrInput(fsDataSinkDir,archivesMRInputDir);
        // process no more than MAX_FILES directories
        if (fileCount >= MAX_FILES) {
          log.info("processDay, reach capacity");
          runArchive(archivesMRInputDir,archivesMROutputDir,finalArchiveOutput);  
          fileCount = 0;
        } else {
          log.info("processDay,modificationDate:" + modificationDate +", skipping: " + fsDataSinkDir.getPath() );
        }
      }
    }    
  }
  
  public void runArchive(String archivesMRInputDir,String archivesMROutputDir,
      String finalArchiveOutput) throws Exception {
    String[] args = new String[3];
    
    
    args[0] = conf.get("archive.grouper","Stream");
    args[1] = archivesMRInputDir + "*/*.done" ;
    args[2] = archivesMROutputDir;
    
    Path pArchivesMRInputDir = new Path(archivesMRInputDir);
    Path pArchivesMROutputDir = new Path(archivesMROutputDir);

    
    if (fs.exists(pArchivesMROutputDir)) {
      log.warn("Deleteing mroutput dir for archive ...");
      fs.delete(pArchivesMROutputDir, true);
    }
    
    log.info("ChukwaArchiveManager processing :" + args[1] + " going to output to " + args[2] );
    int res = ToolRunner.run(this.conf, new ChukwaArchiveBuilder(),args);
    log.info("Archive result: " + res);
    if (res != 0) {
      throw new Exception("Archive result != 0");
    }
   
    if (!finalArchiveOutput.endsWith("/")) {
      finalArchiveOutput +="/";
    }
    String day = pArchivesMRInputDir.getName();
    finalArchiveOutput += day;
    Path pDay = new Path(finalArchiveOutput);
    setup(pDay);
    
    finalArchiveOutput += "/archive_" + System.currentTimeMillis();
    Path pFinalArchiveOutput = new Path(finalArchiveOutput);
    
    log.info("Final move: moving " + pArchivesMROutputDir + " to " + pFinalArchiveOutput);
    
    if (fs.rename(pArchivesMROutputDir, pFinalArchiveOutput ) ) {
      log.info("deleting " + pArchivesMRInputDir);
      fs.delete(pArchivesMRInputDir, true);
    } else {
      log.warn("move to final archive folder failed!");
    }
    

    
  }
  
  public void moveDataSinkFilesToArchiveMrInput(FileStatus fsDataSinkDir,
      String archivesMRInputDir) throws IOException {
    
    if (!archivesMRInputDir.endsWith("/")) {
      archivesMRInputDir +="/";
    }
    
    Path pArchivesMRInputDir = new Path(archivesMRInputDir);
    setup(pArchivesMRInputDir);
    fs.rename(fsDataSinkDir.getPath(), pArchivesMRInputDir);
    log.info("moving " + fsDataSinkDir.getPath() + " to " + pArchivesMRInputDir);
  }
  
  /**
   * Create directory if !exists
   * @param directory
   * @throws IOException
   */
  protected void setup(Path directory) throws IOException {
     if ( ! fs.exists(directory)) {
       fs.mkdirs(directory);
     }
  }
 
}
