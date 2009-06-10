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

package org.apache.hadoop.chukwa.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * 
 * Utility class to move pig output closer to the Demux output <BR>
 * pigDir should looks like:<BR>
 * <UL>
 * <LI> workingDay + ".D" </LI>
 * <LI> workingDay + "_" + workingHour + ".H" </LI>
 * <LI> workingDay + "_" + workingHour + "_" + [0-5] + [0,5] + ".R" </LI>
 * </UL>
 * 
 */
public class PigMover {

  private static Logger log = Logger.getLogger(PigMover.class);

  public static void usage() {
    System.out
        .println("PigMover <cluster> <recordType> <pigDir> <finalOutPutDir>");
    System.exit(-1);
  }

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {

    if (args.length != 5) {
      log.warn("Wrong number of arguments");
      usage();
    }
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    log.info("fs URI:" + fs.getUri());

    String cluster = args[0];
    String recordType = args[1];

    log.info("Cluster:" + cluster);
    log.info("recordType:" + recordType);

    Path rootpigDir = new Path(args[2]);
    log.info("rootpigDir:" + rootpigDir);

    Path dataDir = new Path(args[3]);
    log.info("dataDir:" + dataDir);

    if (!fs.exists(dataDir)) {
      throw new RuntimeException("input directory does not exist.");
    }
    
    String fileName = dataDir.getName();
    log.info("fileName:" + fileName);

    String rootPigPostProcessDir = args[4];
    log.info("chukwaPostProcessDir: [" + rootPigPostProcessDir + "]");

    String finalPigOutputDir = rootPigPostProcessDir + "/pigOutputDir_" + System.currentTimeMillis()
        + "/" + cluster + "/" + recordType;
    log.info("finalPigOutputDir:" + finalPigOutputDir);
    
    Path postProcessDir = new Path(finalPigOutputDir);
    fs.mkdirs(postProcessDir);

    boolean movingDone = true;
    FileStatus[] files = fs.listStatus(dataDir);
    
    for (int i=0;i<files.length;i++) {
      log.info("fileIn:" + files[i].getPath());
      
      Path p = new Path(finalPigOutputDir + "/"+ recordType + "_" + i + "_" + fileName + ".evt");
      log.info("fileOut:" + p);
      if ( fs.rename(files[i].getPath(), p) == false) {
        log.warn("Cannot rename " + files[i].getPath() + " to " + p);
        movingDone = false;
      }
    }
    if (movingDone) {
      log.info("Deleting:" + rootpigDir);
      fs.delete(rootpigDir,true);
    }
  }

}
