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
package org.apache.hadoop.chukwa.dataloader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.chukwa.analysis.salsa.fsm.FSMBuilder;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;

public class FSMDataLoader extends DataLoaderFactory {
  private static Log log = LogFactory.getLog(FSMDataLoader.class);

  protected MetricDataLoader threads[] = null;
  private static String DATA_LOADER_THREAD_LIMIT = "chukwa.data.loader.threads.limit";
  private int size = 1;
  private static CompletionService completion = null;
  private static ExecutorService executor = null;
  private static String[] mappers = {
    "org.apache.hadoop.chukwa.analysis.salsa.fsm.DataNodeClientTraceMapper",
    "org.apache.hadoop.chukwa.analysis.salsa.fsm.TaskTrackerClientTraceMapper",
    "org.apache.hadoop.chukwa.analysis.salsa.fsm.JobHistoryTaskDataMapper"
  };
  
  public FSMDataLoader() {
  }
  
  public void load(ChukwaConfiguration conf, FileSystem fs, FileStatus[] fileList) throws IOException {

    if(executor==null) {
      try {
        this.size = Integer.parseInt(conf.get(DATA_LOADER_THREAD_LIMIT));
      } catch(Exception e) {
        this.size = 1;
      }
      executor = Executors.newFixedThreadPool(size);
    }
    if(completion==null) {
      completion = new ExecutorCompletionService(executor);
    }
    
    try {
      // Locate directory output directories of the current demux, and create a unique directory list.
      HashSet<Path> inputPaths = new HashSet<Path>();
      HashSet<Path> outputPaths = new HashSet<Path>();
      int counter = 0;
      for(int i=0;i<fileList.length;i++) {
        Path temp = fileList[i].getPath().getParent();
        if(!inputPaths.contains(temp)) {
          inputPaths.add(temp);
        }
      }
      String outputDir= conf.get("chukwa.tmp.data.dir")+File.separator+"fsm_"+System.currentTimeMillis()+"_";
      if(inputPaths.size()>0) {
        Configuration fsmConf = new Configuration();
        // Run fsm map reduce job for dn, tt, and jobhist.
        for(String mapper : mappers) {
          String[] args = new String[inputPaths.size()+3];
          args[0]="-in";
          int k=2;
          boolean hasData=false;
          for(Path temp : inputPaths) {
            String tempPath = temp.toUri().toString();
            if((mapper.intern()==mappers[0].intern() && tempPath.indexOf("ClientTraceDetailed")>0) ||
                (mapper.intern()==mappers[1].intern() && tempPath.indexOf("ClientTraceDetailed")>0) ||
                (mapper.intern()==mappers[2].intern() && tempPath.indexOf("TaskData")>0) ||
                (mapper.intern()==mappers[2].intern() && tempPath.indexOf("JobData")>0)) {
              args[k]=tempPath;
              k++;
              hasData=true;
            }
          }
          args[1]=k-2+"";
          fsmConf.set("chukwa.salsa.fsm.mapclass", mapper);
          args[k]=outputDir+mapper;
          Path outputPath = new Path(args[k]);
          outputPaths.add(outputPath);
          if(hasData) {
            int res = ToolRunner.run(fsmConf, new FSMBuilder(), args);
          }
        }
      }
      // Find the mapreduce output and load to MDL.
      for(Path outputPath : outputPaths) {
        Path searchDir = new Path(outputPath.toUri().toString()+"/*/*/*.evt");
        log.info("Search dir:"+searchDir.toUri().toString());
        FileStatus[] outputList = fs.globStatus(searchDir);
        if(outputList!=null) {
          for(int j=0;j<outputList.length;j++) {
            String outputFile = outputList[j].getPath().toUri().toString();
            log.info("FSM -> MDL loading: "+outputFile);
            completion.submit(new MetricDataLoader(conf, fs, outputFile));
            counter++;
          }
        } else {
          log.warn("No output to load.");
        }
      }
      for(int i=0;i<counter;i++) {
        completion.take().get();
      }
      // Clean up mapreduce output of fsm.
      for(Path dir : outputPaths) {
        fs.delete(dir, true);
      }
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new IOException();
    } finally {
    }
  }

  public void shutdown() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(30, TimeUnit.SECONDS);
    executor.shutdownNow();
  }
}
