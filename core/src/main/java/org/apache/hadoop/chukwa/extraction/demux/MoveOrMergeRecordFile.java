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
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MoveOrMergeRecordFile extends Configured implements Tool {
  static ChukwaConfiguration conf = null;
  static FileSystem fs = null;
  static final String HadoopLogDir = "_logs";
  static final String hadoopTempDir = "_temporary";

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), MoveOrMergeRecordFile.class);

    conf.setJobName("Chukwa-MoveOrMergeLogFile");
    conf.setInputFormat(SequenceFileInputFormat.class);

    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    // conf.setPartitionerClass(ChukwaPartitioner.class);
    // conf.setOutputFormat(ChukwaOutputFormat.class);

    conf.setOutputKeyClass(ChukwaRecordKey.class);
    conf.setOutputValueClass(ChukwaRecord.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    FileInputFormat.setInputPaths(conf, args[0]);
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
    return 0;
  }

  static void moveOrMergeOneCluster(Path srcDir, String destDir)
      throws Exception {
    System.out.println("moveOrMergeOneCluster (" + srcDir.getName() + ","
        + destDir + ")");
    FileStatus fstat = fs.getFileStatus(srcDir);

    if (!fstat.isDir()) {
      throw new IOException(srcDir + " is not a directory!");
    } else {
      FileStatus[] datasourceDirectories = fs.listStatus(srcDir);
      for (FileStatus datasourceDirectory : datasourceDirectories) {
        System.out.println(datasourceDirectory.getPath() + " isDir?"
            + datasourceDirectory.isDir());
        if (!datasourceDirectory.isDir()) {
          throw new IOException("Top level should just contains directories :"
              + datasourceDirectory.getPath());
        }

        String dirName = datasourceDirectory.getPath().getName();

        Path destPath = new Path(destDir + "/" + dirName);
        System.out.println("dest directory path: " + destPath);

        if (!fs.exists(destPath)) {
          System.out.println("create datasource directory [" + destDir + "/"
              + dirName + "]");
          fs.mkdirs(destPath);
        }

        FileStatus[] evts = fs.listStatus(datasourceDirectory.getPath(),
            new EventFileFilter());
        for (FileStatus eventFile : evts) {

          Path eventFilePath = eventFile.getPath();
          String filename = eventFilePath.getName();
          System.out.println("src dir File: [" + filename + "]");
          Path destFilePath = new Path(destDir + "/" + dirName + "/" + filename);
          if (!fs.exists(destFilePath)) {
            System.out.println("Moving File: [" + destFilePath + "]");
            // Copy to final Location
            FileUtil.copy(fs, eventFilePath, fs, destFilePath, false, false,
                conf);
          } else {
            System.out.println("Need to merge! : [" + destFilePath + "]");
            String strMrPath = datasourceDirectory.getPath().toString() + "/"
                + "MR_" + System.currentTimeMillis();
            Path mrPath = new Path(strMrPath);
            System.out.println("\t New MR directory : [" + mrPath + "]");
            // Create MR input Dir
            fs.mkdirs(mrPath);
            // Move Input files
            FileUtil.copy(fs, eventFilePath, fs,
                new Path(strMrPath + "/1.evt"), false, false, conf);
            fs.rename(destFilePath, new Path(strMrPath + "/2.evt"));

            // Merge
            String[] mergeArgs = new String[2];
            mergeArgs[0] = strMrPath;
            mergeArgs[1] = strMrPath + "/mrOutput";
            DoMerge merge = new DoMerge(conf, fs, eventFilePath, destFilePath,
                mergeArgs);
            merge.start();
          }
        }
      }
    }

  }

  /**
   * @param args is command line parameters
   * @throws Exception if unable to process data
   */
  public static void main(String[] args) throws Exception {
    conf = new ChukwaConfiguration();
    String fsName = conf.get("writer.hdfs.filesystem");
    fs = FileSystem.get(new URI(fsName), conf);

    Path srcDir = new Path(args[0]);
    String destDir = args[1];

    FileStatus fstat = fs.getFileStatus(srcDir);

    if (!fstat.isDir()) {
      throw new IOException(srcDir + " is not a directory!");
    } else {
      FileStatus[] clusters = fs.listStatus(srcDir);
      // Run a moveOrMerge on all clusters
      String name = null;
      for (FileStatus cluster : clusters) {
        name = cluster.getPath().getName();
        // Skip hadoop outDir
        if ((name.intern() == HadoopLogDir.intern())
            || (name.intern() == hadoopTempDir.intern())) {
          continue;
        }
        moveOrMergeOneCluster(cluster.getPath(), destDir + "/"
            + cluster.getPath().getName());
      }
    }
    System.out.println("Done with moveOrMerge main()");
  }
}


class DoMerge extends Thread {
  ChukwaConfiguration conf = null;
  FileSystem fs = null;
  String[] mergeArgs = new String[2];
  Path destFilePath = null;
  Path eventFilePath = null;

  public DoMerge(ChukwaConfiguration conf, FileSystem fs, Path eventFilePath,
                 Path destFilePath, String[] mergeArgs) {
    this.conf = conf;
    this.fs = fs;
    this.eventFilePath = eventFilePath;
    this.destFilePath = destFilePath;
    this.mergeArgs = mergeArgs;
  }

  @Override
  public void run() {
    System.out.println("\t Running Merge! : output [" + mergeArgs[1] + "]");
    int res;
    try {
      res = ToolRunner.run(new ChukwaConfiguration(),
          new MoveOrMergeRecordFile(), mergeArgs);
      System.out.println("MR exit status: " + res);
      if (res == 0) {
        System.out.println("\t Moving output file : to [" + destFilePath + "]");
        FileUtil.copy(fs, new Path(mergeArgs[1] + "/part-00000"), fs,
            destFilePath, false, false, conf);
        fs.rename(new Path(mergeArgs[1] + "/part-00000"), eventFilePath);
      } else {
        throw new RuntimeException("Error in M/R merge operation!");
      }

    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("Error in M/R merge operation!", e);
    }
  }

}


class EventFileFilter implements PathFilter {
  public boolean accept(Path path) {
    return (path.toString().endsWith(".evt"));
  }
}
