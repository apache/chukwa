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

import java.io.IOException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class MetricDataLoaderPool extends DataLoaderFactory {
  private static Log log = LogFactory.getLog(MetricDataLoaderPool.class);

  protected MetricDataLoader threads[] = null;
  private static String DATA_LOADER_THREAD_LIMIT = "chukwa.data.loader.threads.limit";
  private int size = 1;
  private static CompletionService completion = null;
  private static ExecutorService executor = null;
  
  public MetricDataLoaderPool() {
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
      for(int i=0;i<fileList.length;i++) {
        String filename = fileList[i].getPath().toUri().toString();
        log.info("Processing: "+filename);
        completion.submit(new MetricDataLoader(conf, fs, filename));      
      }
      for(int i=0;i<fileList.length;i++) {
        completion.take().get();
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