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

package org.apache.hadoop.chukwa.extraction.demux;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.chukwa.util.NagiosHelper;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class DemuxManager implements CHUKWA_CONSTANT {  
  static Logger log = Logger.getLogger(DemuxManager.class);

  static int globalErrorcounter = 0;
  static Date firstErrorTime = null;

  protected int ERROR_SLEEP_TIME = 60;
  protected int NO_DATASINK_SLEEP_TIME = 20;

  protected int DEFAULT_MAX_ERROR_COUNT = 6;
  protected int DEFAULT_MAX_FILES_PER_DEMUX = 500;
  protected int DEFAULT_REDUCER_COUNT = 8;
  
  protected int maxPermittedErrorCount = DEFAULT_MAX_ERROR_COUNT;
  protected int demuxReducerCount = 0;
  protected ChukwaConfiguration conf = null;
  protected FileSystem fs = null;
  protected int reprocess = 0;
  protected boolean sendAlert = true;
  
  protected SimpleDateFormat dayTextFormat = new java.text.SimpleDateFormat("yyyyMMdd");
  protected volatile boolean isRunning = true;
  private final static String demuxPath = System.getenv("CHUKWA_HOME")+File.separator+"lib"+File.separator+"demux";

  final private static PathFilter DATA_SINK_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return file.getName().endsWith(".done");
    }     
  };


  public static void main(String[] args) throws Exception {
    DaemonWatcher.createInstance("DemuxManager");
    
    DemuxManager manager = new DemuxManager();
    manager.start();

  }

  public DemuxManager() throws Exception {
    this.conf = new ChukwaConfiguration();
    init();
  }

  public DemuxManager(ChukwaConfiguration conf) throws Exception {
    this.conf = conf;
    init();
  }

  protected void init() throws IOException, URISyntaxException {
    String fsName = conf.get(HDFS_DEFAULT_NAME_FIELD);
    fs = FileSystem.get(new URI(fsName), conf);
  }

  public void shutdown() {
    this.isRunning = false;
  }


  public int getReprocess() {
    return reprocess;
  }

  /**
   * Start the Demux Manager daemon
   * @throws Exception
   */
  public void start() throws Exception {

     String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD, DEFAULT_CHUKWA_ROOT_DIR_NAME);
     if ( ! chukwaRootDir.endsWith("/") ) {
       chukwaRootDir += "/";
     }
     log.info("chukwaRootDir:" + chukwaRootDir);

     String demuxRootDir = chukwaRootDir + DEFAULT_DEMUX_PROCESSING_DIR_NAME;
     String demuxErrorDir = demuxRootDir + DEFAULT_DEMUX_IN_ERROR_DIR_NAME;
     String demuxInputDir = demuxRootDir + DEFAULT_DEMUX_MR_INPUT_DIR_NAME;
     String demuxOutputDir = demuxRootDir + DEFAULT_DEMUX_MR_OUTPUT_DIR_NAME;

     String dataSinkDir = conf.get(CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +DEFAULT_CHUKWA_LOGS_DIR_NAME);
     if ( ! dataSinkDir.endsWith("/") ) {
       dataSinkDir += "/";
     }
     log.info("dataSinkDir:" + dataSinkDir);
     
     String postProcessDir = conf.get(CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +DEFAULT_CHUKWA_POSTPROCESS_DIR_NAME);
     if ( ! postProcessDir.endsWith("/") ) {
       postProcessDir += "/";
     }
     log.info("postProcessDir:" + postProcessDir);
     
     String archiveRootDir = conf.get(CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +DEFAULT_CHUKWA_DATASINK_DIR_NAME);
     if ( ! archiveRootDir.endsWith("/") ) {
       archiveRootDir += "/";
     }
     log.info("archiveRootDir:" + archiveRootDir);
     
     maxPermittedErrorCount = conf.getInt(CHUKWA_DEMUX_MAX_ERROR_COUNT_FIELD,
                                          DEFAULT_MAX_ERROR_COUNT);
     demuxReducerCount = conf.getInt(CHUKWA_DEMUX_REDUCER_COUNT_FIELD, DEFAULT_REDUCER_COUNT);
     log.info("demuxReducerCount:" + demuxReducerCount);
     
     String nagiosHost = conf.get(CHUKWA_NAGIOS_HOST_FIELD);
     int nagiosPort = conf.getInt(CHUKWA_NAGIOS_PORT_FIELD,0);
     String reportingHost = conf.get(CHUKWA_REPORTING_HOST_FIELD);
     
     log.info("Nagios information: nagiosHost:" + nagiosHost + ", nagiosPort:" 
         + nagiosPort + ", reportingHost:" + reportingHost);
     
     
     if (nagiosHost == null || nagiosHost.length() == 0 || nagiosPort == 0 || reportingHost.length() == 0 || reportingHost == null) {
       sendAlert = false;
       log.warn("Alerting is OFF");
     }
     
     boolean demuxReady = false;

     
     while (isRunning) {
       try {
         demuxReady = false;

         if (maxPermittedErrorCount != -1 && globalErrorcounter >= maxPermittedErrorCount) {
           log.warn("==================\nToo many errors (" + globalErrorcounter +
                    "), Bail out!\n==================");
           DaemonWatcher.bailout(-1);
         }
         
         // Check for anomalies
         if (checkDemuxOutputDir(demuxOutputDir) == true) {
           // delete current demux output dir
           if ( deleteDemuxOutputDir(demuxOutputDir) == false ) {
             log.warn("Cannot delete an existing demux output directory!");
             throw new IOException("Cannot move demuxOutput to postProcess!");
           }
           continue;
         } else if (checkDemuxInputDir(demuxInputDir) == true) { // dataSink already there
           reprocess++;

           // Data has been processed more than 3 times ... move to InError directory
           if (reprocess > 3) {
             if (moveDataSinkFilesToDemuxErrorDirectory(demuxInputDir,demuxErrorDir) == false) {
               log.warn("Cannot move dataSink files to DemuxErrorDir!");
               throw new IOException("Cannot move dataSink files to DemuxErrorDir!");
             }
             reprocess = 0;
             continue;
           }

           log.error("Demux inputDir aready contains some dataSink files,"
               + " going to reprocess, reprocessCount=" + reprocess);
           demuxReady = true;
         } else { // standard code path
           reprocess = 0;
           // Move new dataSink Files
           if (moveDataSinkFilesToDemuxInputDirectory(dataSinkDir, demuxInputDir) == true) {
             demuxReady = true; // if any are available
           } else {
             demuxReady = false; // if none
           }
         }

         // start a new demux ?
         if (demuxReady == true) {
          boolean demuxStatus = processData(dataSinkDir, demuxInputDir, demuxOutputDir,
               postProcessDir, archiveRootDir);
          sendDemuxStatusToNagios(nagiosHost,nagiosPort,reportingHost,demuxErrorDir,demuxStatus,null);

          // if demux suceeds, then we reset these.
          if (demuxStatus) {
           globalErrorcounter = 0;
           firstErrorTime = null;
          }
         } else {
           log.info("Demux not ready so going to sleep ...");
           Thread.sleep(NO_DATASINK_SLEEP_TIME * 1000);
         }
       }catch(Throwable e) {
         globalErrorcounter ++;
         if (firstErrorTime == null) firstErrorTime = new Date();

         log.warn("Consecutive error number " + globalErrorcounter +
                  " encountered since " + firstErrorTime, e);
         sendDemuxStatusToNagios(nagiosHost,nagiosPort,reportingHost,demuxErrorDir,false, e.getMessage());
         try { Thread.sleep(ERROR_SLEEP_TIME * 1000); } 
         catch (InterruptedException e1) {/*do nothing*/ }
         init();
       }
     }
   }


   /**
    * Send NSCA status to Nagios
    * @param nagiosHost
    * @param nagiosPort
    * @param reportingHost
    * @param demuxInErrorDir
    * @param demuxStatus
    * @param exception
    */
  protected void sendDemuxStatusToNagios(String nagiosHost,int nagiosPort,String reportingHost,
        String demuxInErrorDir,boolean demuxStatus,String demuxException) {
      
     if (sendAlert == false) {
       return;
     }
      
     boolean demuxInErrorStatus = true;
     String demuxInErrorMsg = "";
     try {
       Path pDemuxInErrorDir = new Path(demuxInErrorDir);
       if ( fs.exists(pDemuxInErrorDir)) {
         FileStatus[] demuxInErrorDirs = fs.listStatus(pDemuxInErrorDir);
         if (demuxInErrorDirs.length == 0) {
           demuxInErrorStatus = false;
         }          
       }
     } catch (Throwable e) {
       demuxInErrorMsg = e.getMessage();
       log.warn(e);
     }
     
     // send Demux status
     if (demuxStatus == true) {
       NagiosHelper.sendNsca(nagiosHost,nagiosPort,reportingHost,"DemuxProcessing","Demux OK",NagiosHelper.NAGIOS_OK);
     } else {
       NagiosHelper.sendNsca(nagiosHost,nagiosPort,reportingHost,"DemuxProcessing","Demux failed. " + demuxException,NagiosHelper.NAGIOS_CRITICAL);
     }
     
     // send DemuxInErrorStatus
     if (demuxInErrorStatus == false) {
       NagiosHelper.sendNsca(nagiosHost,nagiosPort,reportingHost,"DemuxInErrorDirectory","DemuxInError OK",NagiosHelper.NAGIOS_OK);
     } else {
       NagiosHelper.sendNsca(nagiosHost,nagiosPort,reportingHost,"DemuxInErrorDirectory","DemuxInError not empty -" + demuxInErrorMsg,NagiosHelper.NAGIOS_CRITICAL);
     }
     
   }
   
   /**
    * Process Data, i.e. 
    * - run demux
    * - move demux output to postProcessDir
    * - move dataSink file to archiveDir
    * 
    * @param dataSinkDir
    * @param demuxInputDir
    * @param demuxOutputDir
    * @param postProcessDir
    * @param archiveDir
    * @return True iff succeed
    * @throws IOException
    */
    protected boolean processData(String dataSinkDir, String demuxInputDir,
       String demuxOutputDir, String postProcessDir, String archiveDir) throws IOException {

     boolean demuxStatus = false;

     long startTime = System.currentTimeMillis();
     demuxStatus = runDemux(demuxInputDir, demuxOutputDir);
     log.info("Demux Duration: " + (System.currentTimeMillis() - startTime));

     if (demuxStatus == false) {
       log.warn("Demux failed!");
     } else {

       // Move demux output to postProcessDir 
       if (checkDemuxOutputDir(demuxOutputDir)) {
         if (moveDemuxOutputDirToPostProcessDirectory(demuxOutputDir, postProcessDir) == false) {
           log.warn("Cannot move demuxOutput to postProcess! bail out!");
           throw new IOException("Cannot move demuxOutput to postProcess! bail out!");
         } 
       } else {
         log.warn("Demux processing OK but no output");
       }

       // Move DataSink Files to archiveDir
       if (moveDataSinkFilesToArchiveDirectory(demuxInputDir, archiveDir) == false) {
         log.warn("Cannot move datasinkFile to archive! bail out!");
         throw new IOException("Cannot move datasinkFile to archive! bail out!");
       }
     }
     
     return demuxStatus;
   }


   /**
    * Submit and Run demux Job 
    * @param demuxInputDir
    * @param demuxOutputDir
    * @return true id Demux succeed
    */
   protected boolean runDemux(String demuxInputDir, String demuxOutputDir) {
     String[] demuxParams;
     int i=0;
     Demux.addParsers(conf);
     demuxParams = new String[4];
     demuxParams[i++] = "-r";
     demuxParams[i++] = "" + demuxReducerCount;
     demuxParams[i++] = demuxInputDir;
     demuxParams[i++] = demuxOutputDir;
     try {
       return ( 0 == ToolRunner.run(this.conf,new Demux(), demuxParams) );
     } catch (Throwable e) {
       e.printStackTrace();
       globalErrorcounter ++;
       if (firstErrorTime == null) firstErrorTime = new Date();
       log.error("Failed to run demux. Consecutive error number " +
               globalErrorcounter + " encountered since " + firstErrorTime, e);
     }
     return false;
   }



   /**
    * Move dataSink files to Demux input directory
    * @param dataSinkDir
    * @param demuxInputDir
    * @return true if there's any dataSink files ready to be processed
    * @throws IOException
    */
   protected boolean moveDataSinkFilesToDemuxInputDirectory(
       String dataSinkDir, String demuxInputDir) throws IOException {
     Path pDataSinkDir = new Path(dataSinkDir);
     Path pDemuxInputDir = new Path(demuxInputDir);
     log.info("dataSinkDir: " + dataSinkDir);
     log.info("demuxInputDir: " + demuxInputDir);


     boolean containsFile = false;

     FileStatus[] dataSinkFiles = fs.listStatus(pDataSinkDir,DATA_SINK_FILTER);
     if (dataSinkFiles.length > 0) {
       setup(pDemuxInputDir);
     }

     int maxFilesPerDemux = 0;
     for (FileStatus fstatus : dataSinkFiles) {
       boolean rename = fs.rename(fstatus.getPath(),pDemuxInputDir);
       log.info("Moving " + fstatus.getPath() + " to " + pDemuxInputDir +", status is:" + rename);
       maxFilesPerDemux ++;
       containsFile = true;
       if (maxFilesPerDemux >= DEFAULT_MAX_FILES_PER_DEMUX) {
         log.info("Max File per Demux reached:" + maxFilesPerDemux);
         break;
       }
     }
     return containsFile;
   }




   /**
    * Move sourceFolder inside destFolder
    * @param dataSinkDir : ex chukwa/demux/inputDir
    * @param demuxErrorDir : ex /chukwa/demux/inError
    * @return true if able to move chukwa/demux/inputDir to /chukwa/demux/inError/<YYYYMMDD>/demuxInputDirXXX
    * @throws IOException
    */
   protected boolean moveDataSinkFilesToDemuxErrorDirectory(
       String dataSinkDir, String demuxErrorDir) throws IOException {
     demuxErrorDir += "/" + dayTextFormat.format(System.currentTimeMillis());
     return moveFolder(dataSinkDir,demuxErrorDir,"demuxInputDir");
   }

   /**
    * Move sourceFolder inside destFolder
    * @param demuxInputDir: ex chukwa/demux/inputDir
    * @param archiveDirectory: ex /chukwa/archives
    * @return true if able to move chukwa/demux/inputDir to /chukwa/archives/raw/<YYYYMMDD>/dataSinkDirXXX
    * @throws IOException
    */
   protected boolean moveDataSinkFilesToArchiveDirectory(
       String demuxInputDir, String archiveDirectory) throws IOException {
     archiveDirectory += "/" + dayTextFormat.format(System.currentTimeMillis());
     return moveFolder(demuxInputDir,archiveDirectory,"dataSinkDir");
   }

   /**
    * Move sourceFolder inside destFolder
    * @param demuxOutputDir: ex chukwa/demux/outputDir 
    * @param postProcessDirectory: ex /chukwa/postProcess
    * @return true if able to move chukwa/demux/outputDir to /chukwa/postProcess/demuxOutputDirXXX
    * @throws IOException 
    */
   protected  boolean moveDemuxOutputDirToPostProcessDirectory(
       String demuxOutputDir, String postProcessDirectory) throws IOException {
     return moveFolder(demuxOutputDir,postProcessDirectory,"demuxOutputDir");
   }


   /**
    * Test if demuxInputDir exists
    * @param demuxInputDir
    * @return true if demuxInputDir exists
    * @throws IOException
    */
   protected boolean checkDemuxInputDir(String demuxInputDir)
   throws IOException {
     return dirExists(demuxInputDir);
   }

   /**
    * Test if demuxOutputDir exists
    * @param demuxOutputDir
    * @return true if demuxOutputDir exists
    * @throws IOException
    */
   protected boolean checkDemuxOutputDir(String demuxOutputDir)
   throws IOException {
     return dirExists(demuxOutputDir);
   }


   /**
    * Delete DemuxOutput directory
    * @param demuxOutputDir
    * @return true if succeed
    * @throws IOException
    */
   protected boolean deleteDemuxOutputDir(String demuxOutputDir) throws IOException
   {
     return fs.delete(new Path(demuxOutputDir), true);
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

    /** 
     * Check if source exists and if source is a directory
     * @param f source file
     */
   protected boolean dirExists(String directory) throws IOException {
      Path pDirectory = new Path(directory);
      return (fs.exists(pDirectory) && fs.getFileStatus(pDirectory).isDir());
    }

    /**
     * Move sourceFolder inside destFolder
     * @param srcDir
     * @param destDir
     * @return
     * @throws IOException
     */ 
   protected boolean moveFolder(String srcDir,String destDir, String prefix) throws IOException {
      if (!destDir.endsWith("/")) {
        destDir +="/";
      }
      Path pSrcDir = new Path(srcDir);
      Path pDestDir = new Path(destDir );
      setup(pDestDir);
      destDir += prefix +"_" +System.currentTimeMillis();
      Path pFinalDestDir = new Path(destDir );

      return fs.rename(pSrcDir, pFinalDestDir);
    }
}
