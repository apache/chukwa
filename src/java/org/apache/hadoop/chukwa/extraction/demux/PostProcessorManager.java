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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Collection;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.dataloader.DataLoaderFactory;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.datatrigger.TriggerAction;
import org.apache.hadoop.chukwa.datatrigger.TriggerEvent;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

public class PostProcessorManager implements CHUKWA_CONSTANT{
  static Logger log = Logger.getLogger(PostProcessorManager.class);
  
  protected static HashMap<String, String> dataSources = new HashMap<String, String>();
  public static int errorCount = 0;
  
  protected int ERROR_SLEEP_TIME = 60;
  protected ChukwaConfiguration conf = null;
  protected FileSystem fs = null;
  protected volatile boolean isRunning = true;

  private static final int DEFAULT_MAX_ERROR_COUNT = 4;
  
  final private static PathFilter POST_PROCESS_DEMUX_DIR_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return ( file.getName().startsWith("demuxOutputDir") || file.getName().startsWith("pigOutputDir"));
    }     
  };

  
  public PostProcessorManager() throws Exception {
    this.conf = new ChukwaConfiguration();
    init();
  }
  
  public PostProcessorManager(ChukwaConfiguration conf) throws Exception {
    this.conf = conf;
    init();
  }
  
  protected void init() throws IOException, URISyntaxException {
    String fsName = conf.get(HDFS_DEFAULT_NAME_FIELD);
    fs = FileSystem.get(new URI(fsName), conf);
  }
  
  public static void main(String[] args) throws Exception {
 
    DaemonWatcher.createInstance("PostProcessorManager");
    

    
    PostProcessorManager postProcessorManager = new PostProcessorManager();
    postProcessorManager.start();
  }
  
  public void shutdown() {
    this.isRunning = false;
  }
  
  public void start() {
    
    String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD, "/chukwa/");
    if ( ! chukwaRootDir.endsWith("/") ) {
      chukwaRootDir += "/";
    }
    log.info("chukwaRootDir:" + chukwaRootDir);
    
    String postProcessDir = conf.get(CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +DEFAULT_CHUKWA_POSTPROCESS_DIR_NAME);
    if ( ! postProcessDir.endsWith("/") ) {
      postProcessDir += "/";
    }
    
    String chukwaRootReposDir = conf.get(CHUKWA_ROOT_REPOS_DIR_FIELD, chukwaRootDir +DEFAULT_REPOS_DIR_NAME);
    if ( ! chukwaRootReposDir.endsWith("/") ) {
      chukwaRootReposDir += "/";
    }
 
    String chukwaPostProcessInErrorDir = conf.get(CHUKWA_POSTPROCESS_IN_ERROR_DIR_FIELD, chukwaRootDir +DEFAULT_POSTPROCESS_IN_ERROR_DIR_NAME);
    if ( ! chukwaPostProcessInErrorDir.endsWith("/") ) {
      chukwaPostProcessInErrorDir += "/";
    }

    int maxPermittedErrorCount = conf.getInt(CHUKWA_POSTPROCESS_MAX_ERROR_COUNT_FIELD,
                                             DEFAULT_MAX_ERROR_COUNT);

    
    dataSources = new HashMap<String, String>();
    Path postProcessDirectory = new Path(postProcessDir);
    while (isRunning) {
      
      if (maxPermittedErrorCount != -1 && errorCount >= maxPermittedErrorCount) {
        log.warn("==================\nToo many errors (" + errorCount +
                 "), Bail out!\n==================");
        DaemonWatcher.bailout(-1);
      }

      try {
        FileStatus[] demuxOutputDirs = fs.listStatus(postProcessDirectory,POST_PROCESS_DEMUX_DIR_FILTER);
        List<String> directories = new ArrayList<String>();
        for (FileStatus fstatus : demuxOutputDirs) {
          directories.add(fstatus.getPath().getName());
        }
        
        if (demuxOutputDirs.length == 0) {
          try { Thread.sleep(10*1000);} catch(InterruptedException e) { /* do nothing*/}
          continue;
        }
        
        Collections.sort(directories);
        
        String directoryToBeProcessed = null;
        long start = 0;
        
        for(String directory : directories) {
          directoryToBeProcessed = postProcessDirectory + "/"+ directory;
          
          log.info("PostProcess Start, directory:" + directoryToBeProcessed);
          start = System.currentTimeMillis();
         
          try {
            if ( processDataLoaders(directoryToBeProcessed) == true) {
              Path[] destFiles = movetoMainRepository(
                directoryToBeProcessed,chukwaRootReposDir);
              if (destFiles != null && destFiles.length > 0) {
                deleteDirectory(directoryToBeProcessed);
                log.info("PostProcess Stop, directory:" + directoryToBeProcessed);
                log.info("processDemuxOutput Duration:" + (System.currentTimeMillis() - start));
                processPostMoveTriggers(destFiles);
                continue;
              }
              } else {
                  log.warn("Error in processDemuxOutput for :" + directoryToBeProcessed + ". Will try again.");
                  if (errorCount > 3)
                      moveToInErrorDirectory(directoryToBeProcessed,directory,chukwaPostProcessInErrorDir); 
                  else 
                      errorCount++;
                  continue;                
              
            }
            
            // if we are here it's because something bad happen during processing
            log.warn("Error in processDemuxOutput for :" + directoryToBeProcessed);
            moveToInErrorDirectory(directoryToBeProcessed,directory,chukwaPostProcessInErrorDir); 
          } catch (Throwable e) {
            log.warn("Error in processDemuxOutput:" ,e);
          } 
        }
       
      } catch (Throwable e) {
        errorCount ++;
        log.warn(e);
        try { Thread.sleep(ERROR_SLEEP_TIME * 1000); } 
        catch (InterruptedException e1) {/*do nothing*/ }
      }
    }
  }
  
  public boolean processDataLoaders(String directory) throws IOException {
    long start = System.currentTimeMillis();
    try {
      String[] classes = conf.get(POST_DEMUX_DATA_LOADER,"org.apache.hadoop.chukwa.dataloader.MetricDataLoaderPool,org.apache.hadoop.chukwa.dataloader.FSMDataLoader").split(",");
      for(String dataLoaderName : classes) {
        Class<? extends DataLoaderFactory> dl = (Class<? extends DataLoaderFactory>) Class.forName(dataLoaderName);
        java.lang.reflect.Constructor<? extends DataLoaderFactory> c =
            dl.getConstructor();
        DataLoaderFactory dataloader = c.newInstance();
        
          //DataLoaderFactory dataLoader = (DataLoaderFactory) Class.
          //    forName(dataLoaderName).getConstructor().newInstance();
        log.info(dataLoaderName+" processing: "+directory);
        StringBuilder dirSearch = new StringBuilder();
        dirSearch.append(directory);
        dirSearch.append("/*/*/*.evt");
        Path demuxDir = new Path(dirSearch.toString());
        FileStatus[] events = fs.globStatus(demuxDir);
        dataloader.load(conf, fs, events);
      }
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      return false;
    }
    log.info("loadData Duration:" + (System.currentTimeMillis() - start));
    return true;
  }

  public boolean processPostMoveTriggers(Path[] files) throws IOException {
    long start = System.currentTimeMillis();
    try {
      String actions = conf.get(POST_DEMUX_SUCCESS_ACTION, null);
      if (actions == null || actions.trim().length() == 0) {
        return true;
      }
      log.info("PostProcess firing postMoveTriggers");

      String[] classes = actions.trim().split(",");
      for(String actionName : classes) {
        Class<? extends TriggerAction> actionClass =
            (Class<? extends TriggerAction>) Class.forName(actionName);
        java.lang.reflect.Constructor<? extends TriggerAction> c =
            actionClass.getConstructor();
        TriggerAction action = c.newInstance();

        log.info(actionName + " handling " + files.length + " events");

        //send the files that were just added benieth the repos/ dir.
        FileStatus[] events = fs.listStatus(files);
        action.execute(conf, fs, events, TriggerEvent.POST_DEMUX_SUCCESS);
      }
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      return false;
    }
    log.info("postMoveTriggers Duration:" + (System.currentTimeMillis() - start));
    return true;
  }

  public Path[] movetoMainRepository(String sourceDirectory,String repoRootDirectory) throws Exception {
    long start = System.currentTimeMillis();
    Path[] destFiles = MoveToRepository.doMove(new Path(sourceDirectory),repoRootDirectory);
    log.info("movetoMainRepository Duration:" + (System.currentTimeMillis() - start));
    return destFiles;
  }
  
  public boolean moveToInErrorDirectory(String sourceDirectory,String dirName,String inErrorDirectory) throws Exception {
    Path inErrorDir = new Path(inErrorDirectory);
    if (!fs.exists(inErrorDir)) {
      fs.mkdirs(inErrorDir);
    }
    
    if (inErrorDirectory.endsWith("/")) {
      inErrorDirectory += "/";
    }
    String finalInErrorDirectory = inErrorDirectory + dirName + "_" + System.currentTimeMillis();
    fs.rename(new Path(sourceDirectory), new Path(finalInErrorDirectory));
    log.warn("Error in postProcess  :" + sourceDirectory + " has been moved to:" + finalInErrorDirectory);
    return true;
  }
  
  public boolean deleteDirectory(String directory) throws IOException {
   return fs.delete(new Path(directory), true);
  }
  
}
