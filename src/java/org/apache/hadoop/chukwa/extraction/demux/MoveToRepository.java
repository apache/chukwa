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
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

// TODO
// First version of the Spill
// need some polishing

public class MoveToRepository {
  static Logger log = Logger.getLogger(MoveToRepository.class);

  static ChukwaConfiguration conf = null;
  static FileSystem fs = null;
  static SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
  static Calendar calendar = Calendar.getInstance();

  static Collection<Path> processClusterDirectory(Path srcDir, String destDir)
      throws Exception {
    log.info("processClusterDirectory (" + srcDir.getName() + "," + destDir
        + ")");
    FileStatus fstat = fs.getFileStatus(srcDir);
    Collection<Path> destFiles = new HashSet<Path>();

    if (!fstat.isDir()) {
      throw new IOException(srcDir + " is not a directory!");
    } else {
      FileStatus[] datasourceDirectories = fs.listStatus(srcDir);

      for (FileStatus datasourceDirectory : datasourceDirectories) {
        log.info(datasourceDirectory.getPath() + " isDir?"
            + datasourceDirectory.isDir());
        if (!datasourceDirectory.isDir()) {
          throw new IOException(
              "Top level datasource directory should be a directory :"
                  + datasourceDirectory.getPath());
        }

        String dirName = datasourceDirectory.getPath().getName();
        Path destPath = new Path(destDir + "/" + dirName);
        log.info("dest directory path: " + destPath);
        log.info("processClusterDirectory processing Datasource: (" + dirName
            + ")");
        destFiles.addAll(processDatasourceDirectory(srcDir.getName(),
            datasourceDirectory.getPath(), destDir + "/" + dirName));
      }
    }
    return destFiles;
  }

  static Collection<Path> processDatasourceDirectory(String cluster, Path srcDir,
      String destDir) throws Exception {
    Collection<Path> destFiles = new HashSet<Path>();
    String fileName = null;
    int fileDay = 0;
    int fileHour = 0;
    int fileMin = 0;

    FileStatus[] recordFiles = fs.listStatus(srcDir);
    for (FileStatus recordFile : recordFiles) {
      // dataSource_20080915_18_15.1.evt
      // <datasource>_<yyyyMMdd_HH_mm>.1.evt

      fileName = recordFile.getPath().getName();
      log.info("processDatasourceDirectory processing RecordFile: (" + fileName
          + ")");
      log.info("fileName: " + fileName);

      int l = fileName.length();
      String dataSource = srcDir.getName();
      log.info("Datasource: " + dataSource);

      if (fileName.endsWith(".D.evt")) {
        // Hadoop_dfs_datanode_20080919.D.evt

        fileDay = Integer.parseInt(fileName.substring(l - 14, l - 6));
        Path destFile = writeRecordFile(destDir + "/" + fileDay + "/",
            recordFile.getPath(), dataSource + "_" + fileDay);
        if (destFile != null) {
          destFiles.add(destFile);
        }
      } else if (fileName.endsWith(".H.evt")) {
        // Hadoop_dfs_datanode_20080925_1.H.evt
        // Hadoop_dfs_datanode_20080925_12.H.evt

        String day = null;
        String hour = null;
        if (fileName.charAt(l - 8) == '_') {
          day = fileName.substring(l - 16, l - 8);
          log.info("day->" + day);
          hour = "" + fileName.charAt(l - 7);
          log.info("hour->" + hour);
        } else {
          day = fileName.substring(l - 17, l - 9);
          log.info("day->" + day);
          hour = fileName.substring(l - 8, l - 6);
          log.info("hour->" + hour);
        }
        fileDay = Integer.parseInt(day);
        fileHour = Integer.parseInt(hour);
        // rotate there so spill
        Path destFile = writeRecordFile(destDir + "/" + fileDay + "/" + fileHour + "/",
            recordFile.getPath(), dataSource + "_" + fileDay + "_" + fileHour);
        if (destFile != null) {
          destFiles.add(destFile);
        }
        // mark this directory for daily rotate
        addDirectory4Rolling(true, fileDay, fileHour, cluster, dataSource);
      } else if (fileName.endsWith(".R.evt")) {
        if (fileName.charAt(l - 11) == '_') {
          fileDay = Integer.parseInt(fileName.substring(l - 19, l - 11));
          fileHour = Integer.parseInt("" + fileName.charAt(l - 10));
          fileMin = Integer.parseInt(fileName.substring(l - 8, l - 6));
        } else {
          fileDay = Integer.parseInt(fileName.substring(l - 20, l - 12));
          fileHour = Integer.parseInt(fileName.substring(l - 11, l - 9));
          fileMin = Integer.parseInt(fileName.substring(l - 8, l - 6));
        }

        log.info("fileDay: " + fileDay);
        log.info("fileHour: " + fileHour);
        log.info("fileMin: " + fileMin);
        Path destFile = writeRecordFile(destDir + "/" + fileDay + "/" + fileHour + "/"
            + fileMin, recordFile.getPath(), dataSource + "_" + fileDay + "_"
            + fileHour + "_" + fileMin);
        if (destFile != null) {
          destFiles.add(destFile);
        }
        // mark this directory for hourly rotate
        addDirectory4Rolling(false, fileDay, fileHour, cluster, dataSource);
      } else {
        throw new RuntimeException("Wrong fileName format! [" + fileName + "]");
      }
    }

    return destFiles;
  }

  static void addDirectory4Rolling(boolean isDailyOnly, int day, int hour,
      String cluster, String dataSource) throws IOException {
    // TODO get root directory from config
    String rollingDirectory = "/chukwa/rolling/";

    Path path = new Path(rollingDirectory + "/daily/" + day + "/" + cluster
        + "/" + dataSource);
    if (!fs.exists(path)) {
      fs.mkdirs(path);
    }

    if (!isDailyOnly) {
      path = new Path(rollingDirectory + "/hourly/" + day + "/" + hour + "/"
          + cluster + "/" + dataSource);
      if (!fs.exists(path)) {
        fs.mkdirs(path);
      }
    }
  }

  static Path writeRecordFile(String destDir, Path recordFile, String fileName)
      throws IOException {
    boolean done = false;
    int count = 1;
    do {
      Path destDirPath = new Path(destDir);
      Path destFilePath = new Path(destDir + "/" + fileName + "." + count
          + ".evt");

      if (!fs.exists(destDirPath)) {
        fs.mkdirs(destDirPath);
        log.info(">>>>>>>>>>>> create Dir" + destDirPath);
      }

      if (!fs.exists(destFilePath)) {
        log.info(">>>>>>>>>>>> Before Rename" + recordFile + " -- "
            + destFilePath);
        boolean rename = fs.rename(recordFile,destFilePath);
        done = true;
        log.info(">>>>>>>>>>>> after Rename" + destFilePath + " , rename:"+rename);
        return destFilePath;
      } 
      count++;

      if (count > 1000) {
        log.warn("too many files in this directory: " + destDir);
      }
    } while (!done);

    return null;
  }

  static boolean checkRotate(String directoryAsString,
      boolean createDirectoryIfNotExist) throws IOException {
    Path directory = new Path(directoryAsString);
    boolean exist = fs.exists(directory);

    if (!exist) {
      if (createDirectoryIfNotExist == true) {
        fs.mkdirs(directory);
      }
      return false;
    } else {
      return fs.exists(new Path(directoryAsString + "/rotateDone"));
    }
  }

  public static Path[] doMove(Path srcDir, String destDir) throws Exception {
    conf = new ChukwaConfiguration();
    String fsName = conf.get("writer.hdfs.filesystem");
    fs = FileSystem.get(new URI(fsName), conf);
    log.info("Start MoveToRepository doMove()");

    FileStatus fstat = fs.getFileStatus(srcDir);

    Collection<Path> destinationFiles = new HashSet<Path>();
    if (!fstat.isDir()) {
      throw new IOException(srcDir + " is not a directory!");
    } else {
      FileStatus[] clusters = fs.listStatus(srcDir);
      // Run a moveOrMerge on all clusters
      String name = null;
      for (FileStatus cluster : clusters) {
        name = cluster.getPath().getName();
        // Skip hadoop M/R outputDir
        if (name.startsWith("_")) {
          continue;
        }
        log.info("main procesing Cluster (" + cluster.getPath().getName() + ")");
        destinationFiles.addAll(processClusterDirectory(cluster.getPath(),
            destDir + "/" + cluster.getPath().getName()));

        // Delete the demux's cluster dir
        FileUtil.fullyDelete(fs, cluster.getPath());
      }
    }

    log.info("Done with MoveToRepository doMove()");
    return destinationFiles.toArray(new Path[destinationFiles.size()]);
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    Path srcDir = new Path(args[0]);
    String destDir = args[1];
    doMove(srcDir, destDir);
  }

}
