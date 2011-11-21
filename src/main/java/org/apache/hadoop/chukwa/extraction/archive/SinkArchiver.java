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

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * A lightweight tool for archiving, suitable for small-to-medium-size Chukwa
 * deployments that don't use Demux.
 * Grabs everything in the data sink, runs the Archiver MapReduce job,
 * then promotes output to the archive dir.
 * 
 * Input is determined by conf option chukwaArchiveDir; defaults to
 *   /chukwa/logs
 *   
 *   Uses /chukwa/archivesProcessing/mr[Input/Output] as tmp storage
 *   
 *   Outputs to /chukwa/archives
 * 
 */
public class SinkArchiver implements CHUKWA_CONSTANT {
  
  final public static PathFilter DATA_SINK_FILTER = new PathFilter() {
    public boolean accept(Path file) {
      return file.getName().endsWith(".done");
    }     
  };
  
  static Logger log = Logger.getLogger(SinkArchiver.class);
  
  public static void main(String[] args) {
    try {
      Configuration conf = new ChukwaConfiguration();
      if(conf.get(ChukwaArchiveDataTypeOutputFormat.GROUP_BY_CLUSTER_OPTION_NAME) == null )
        conf.set(ChukwaArchiveDataTypeOutputFormat.GROUP_BY_CLUSTER_OPTION_NAME, "true");
      FileSystem fs = FileSystem.get(conf);
      SinkArchiver archiver = new SinkArchiver();
      archiver.exec(fs, conf);    
    } catch(IOException e) {
      e.printStackTrace();
    }
  }
  

  /*
   * Pull most of the logic into instance methods so that we can
   * more easily unit-test, by altering passed-in configuration.
   */
  public void exec(FileSystem fs, Configuration conf) {
    try {
      
      String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD, DEFAULT_CHUKWA_ROOT_DIR_NAME);
      if ( ! chukwaRootDir.endsWith("/") ) {
        chukwaRootDir += "/";
      }
      String archiveSource = conf.get(CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +DEFAULT_CHUKWA_LOGS_DIR_NAME);
      if ( ! archiveSource.endsWith("/") ) {
        archiveSource += "/";
      }
      String archivesRootProcessingDir = chukwaRootDir + ARCHIVES_PROCESSING_DIR_NAME;
      
      //String archivesErrorDir = archivesRootProcessingDir + ARCHIVES_IN_ERROR_DIR_NAME;
      String archivesMRInputDir = archivesRootProcessingDir + ARCHIVES_MR_INPUT_DIR_NAME;
      String archivesMROutputDir = archivesRootProcessingDir+ ARCHIVES_MR_OUTPUT_DIR_NAME;

      Path pSource = new Path(archiveSource);
      
      Path pMRInputDir = new Path(archivesMRInputDir);
      if(!fs.exists(pMRInputDir))
        fs.mkdirs(pMRInputDir);
      
      Path pOutputDir = new Path(archivesMROutputDir);
      if(!fs.exists(pOutputDir))
        fs.mkdirs(pOutputDir);
      
      if(fs.listStatus(pOutputDir).length == 0)
        fs.delete(pOutputDir, true);
      Path archive = new Path(chukwaRootDir + "archive");
      
      selectInputs(fs, pSource, pMRInputDir);

      int result = runMapRedJob(conf, archivesMRInputDir, archivesMROutputDir);
      if(result == 0) { //success, so empty input dir
        fs.delete(pMRInputDir, true);
      }
      
      if(!fs.exists(archive)) {
        fs.mkdirs(archive);
      }
      FileStatus[] files = fs.listStatus(pOutputDir);
      for(FileStatus f: files) {
        if(!f.getPath().getName().endsWith("_logs"))
          promoteAndMerge(fs, f.getPath(), archive);
      }

      fs.delete(pOutputDir, true);
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private void selectInputs(FileSystem fs, Path pSource,
      Path pMRInputDir) throws IOException {
    
    FileStatus[] dataSinkFiles = fs.listStatus(pSource, DATA_SINK_FILTER);
    for(FileStatus fstatus: dataSinkFiles) {
      boolean rename = fs.rename(fstatus.getPath(),pMRInputDir);
      log.info("Moving " + fstatus.getPath() + " to " + pMRInputDir 
          +", status is: " + rename);
    }
    
  }

  public int runMapRedJob(Configuration conf, String in, String out)
    throws Exception {
    String grouper = conf.get("archive.grouper","DataType");
    String[] args = new String[] {grouper, in, out};
    int res = ToolRunner.run(conf, new ChukwaArchiveBuilder(),
        args);
    return res;
  }
  /**
   * Merges the contents of src into dest.
   * If src is a file, moves it to dest.
   * 
   * @param fs the filesystem in question
   * @param src a file or directory to merge into dest
   * @param dest a directory to merge into
   * @throws IOException
   */
  public void promoteAndMerge(FileSystem fs, Path src, Path dest) 
  throws IOException {
    FileStatus stat = fs.getFileStatus(src);
    String baseName = src.getName();
    Path target = new Path(dest, baseName);
    if(!fs.exists(target)) {
      fs.rename(src, dest);
      System.out.println("moving " + src + " to " + dest);
    } else if(stat.isDir()) {//recurse
      FileStatus[] files = fs.listStatus(src);
      for(FileStatus f: files) {
        promoteAndMerge(fs, f.getPath(), target);
      }
    } else { //append a number to unique-ify filename
      int i=0;
      do {
        //FIXME: can make this more generic
        String newName;
        if(baseName.endsWith(".arc")) {
          newName = baseName.substring(0, baseName.length() - 4) + "-"+i+".arc";
        }
        else
          newName = baseName+"-"+i;
        target = new Path(dest, newName);
      } while(fs.exists(target));
      fs.rename(src, target);
      System.out.println("promoting " + src + " to " + target);
    }

  }
}
