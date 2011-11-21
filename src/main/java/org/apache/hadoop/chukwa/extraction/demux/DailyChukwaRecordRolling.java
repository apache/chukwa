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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.PidFile;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

// TODO do an abstract class for all rolling 
public class DailyChukwaRecordRolling extends Configured implements Tool {
  static Logger log = Logger.getLogger(DailyChukwaRecordRolling.class);

  static SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyyMMdd");
  static ChukwaConfiguration conf = null;
  static FileSystem fs = null;
  static final String HadoopLogDir = "_logs";
  static final String hadoopTempDir = "_temporary";

  static boolean rollInSequence = true;
  static boolean deleteRawdata = false;

  public static void usage() {
    System.err
        .println("usage: java org.apache.hadoop.chukwa.extraction.demux.DailyChukwaRecordRolling rollInSequence <True/False> deleteRawdata <True/False>");
    System.exit(-1);
  }

  public static boolean hourlyRolling(String dailyStreamDirectory) {
   
    Path pHour = null;
    try {
      log.info("Checking for HourlyRolling in " + dailyStreamDirectory);
      
      for (int i=0;i<24;i++) {
        pHour = new Path(dailyStreamDirectory + "/" + i);
        if (! fs.exists(pHour)) {
          log.info("HourlyData is missing for:" + pHour);
          continue;
        } else {
          FileStatus[] files = fs.listStatus(pHour);
          boolean containsHourly = false;
          for(FileStatus file: files) {
            log.info("Debug checking" + file.getPath());
            if (file.getPath().getName().indexOf("_HourlyDone_") > 0) {
              containsHourly = true;
              break;
            }
          }
          if (containsHourly == false) {
            log.info("HourlyDone is missing for : " + pHour);
            return false;
          }
        }
      }
      return true;
    }catch(Exception e) {
      e.printStackTrace();
      return false;
    }
  }
  public static void buildDailyFiles(String chukwaMainRepository,
      String tempDir, String rollingFolder, int workingDay) throws IOException {
    // process
    
    boolean alldone = true;
    
    Path dayPath = new Path(rollingFolder + "/daily/" + workingDay);
    FileStatus[] clustersFS = fs.listStatus(dayPath);
    for (FileStatus clusterFs : clustersFS) {
      String cluster = clusterFs.getPath().getName();

      Path dataSourceClusterHourPaths = new Path(rollingFolder + "/daily/"
          + workingDay + "/" + cluster);
      FileStatus[] dataSourcesFS = fs.listStatus(dataSourceClusterHourPaths);
      for (FileStatus dataSourceFS : dataSourcesFS) {
        String dataSource = dataSourceFS.getPath().getName();
        // Repo path = reposRootDirectory/<cluster>/<day>/*/*.evt


        // put the rotate flag
        fs.mkdirs(new Path(chukwaMainRepository + "/" + cluster + "/"
            + dataSource + "/" + workingDay + "/rotateDone"));
        
        if (hourlyRolling(chukwaMainRepository + "/" + cluster + "/" + dataSource + "/" + workingDay) == false) {
          log.warn("Skipping this directory, hourly not done. " + chukwaMainRepository + "/" + cluster + "/"
            + dataSource + "/" + workingDay );
          alldone = false;
          continue;
        } 
        
        log.info("Running Daily rolling for " + chukwaMainRepository + "/" + cluster + "/"
            + dataSource + "/" + workingDay + "/rotateDone");
        
        // rotate
        // Merge
        String[] mergeArgs = new String[5];
        // input
        mergeArgs[0] = chukwaMainRepository + "/" + cluster + "/" + dataSource
            + "/" + workingDay + "/[0-9]*/*.evt";
        // temp dir
        mergeArgs[1] = tempDir + "/" + cluster + "/" + dataSource + "/"
            + workingDay + "_" + System.currentTimeMillis();
        // final output dir
        mergeArgs[2] = chukwaMainRepository + "/" + cluster + "/" + dataSource
            + "/" + workingDay;
        // final output fileName
        mergeArgs[3] = dataSource + "_DailyDone_"  + workingDay;
        // delete rolling directory
        mergeArgs[4] = rollingFolder + "/daily/" + workingDay + "/" + cluster
            + "/" + dataSource;

        log.info("DailyChukwaRecordRolling 0: " + mergeArgs[0]);
        log.info("DailyChukwaRecordRolling 1: " + mergeArgs[1]);
        log.info("DailyChukwaRecordRolling 2: " + mergeArgs[2]);
        log.info("DailyChukwaRecordRolling 3: " + mergeArgs[3]);
        log.info("DailyChukwaRecordRolling 4: " + mergeArgs[4]);

        RecordMerger merge = new RecordMerger(conf, fs,
            new DailyChukwaRecordRolling(), mergeArgs, deleteRawdata);
        List<RecordMerger> allMerge = new ArrayList<RecordMerger>();
        if (rollInSequence) {
          merge.run();
        } else {
          allMerge.add(merge);
          merge.start();
        }

        // join all Threads
        if (!rollInSequence) {
          while (allMerge.size() > 0) {
            RecordMerger m = allMerge.remove(0);
            try {
              m.join();
            } catch (InterruptedException e) {
            }
          }
        } // End if (!rollInSequence)

        // Delete the processed dataSourceFS
          FileUtil.fullyDelete(fs, dataSourceFS.getPath());

      } // End for(FileStatus dataSourceFS : dataSourcesFS)

      // Delete the processed clusterFs
      if (alldone == true) {
        FileUtil.fullyDelete(fs, clusterFs.getPath());
      }
      

    } // End for(FileStatus clusterFs : clustersFS)

    // Delete the processed dayPath
    if (alldone == true) {
      FileUtil.fullyDelete(fs, dayPath);
    }
    
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    
    DaemonWatcher.createInstance("DailyChukwaRecordRolling");
    
    conf = new ChukwaConfiguration();
    String fsName = conf.get("writer.hdfs.filesystem");
    fs = FileSystem.get(new URI(fsName), conf);

    // TODO read from config
    String rollingFolder = "/chukwa/rolling/";
    String chukwaMainRepository = "/chukwa/repos/";
    String tempDir = "/chukwa/temp/dailyRolling/";

    // TODO do a real parameter parsing
    if (args.length != 4) {
      usage();
    }

    if (!args[0].equalsIgnoreCase("rollInSequence")) {
      usage();
    }

    if (!args[2].equalsIgnoreCase("deleteRawdata")) {
      usage();
    }

    if (args[1].equalsIgnoreCase("true")) {
      rollInSequence = true;
    } else {
      rollInSequence = false;
    }

    if (args[3].equalsIgnoreCase("true")) {
      deleteRawdata = true;
    } else {
      deleteRawdata = false;
    }

    log.info("rollInSequence: " + rollInSequence);
    log.info("deleteRawdata: " + deleteRawdata);

    Calendar calendar = Calendar.getInstance();
    int currentDay = Integer.parseInt(sdf.format(calendar.getTime()));
    int currentHour = calendar.get(Calendar.HOUR_OF_DAY);
    log.info("CurrentDay: " + currentDay);
    log.info("currentHour" + currentHour);

    Path rootFolder = new Path(rollingFolder + "/daily/");

    FileStatus[] daysFS = fs.listStatus(rootFolder);
    for (FileStatus dayFS : daysFS) {
      try {
        int workingDay = Integer.parseInt(dayFS.getPath().getName());
        log.info("Daily working on :" + workingDay);
        if (workingDay < currentDay) {
          
          try {
            buildDailyFiles(chukwaMainRepository, tempDir, rollingFolder,
                workingDay);
          } catch(Throwable e) {
            e.printStackTrace();
            log.warn("Daily rolling failed on :" + rollingFolder +"/" + workingDay  ) ;
          }
          
        } // End if ( workingDay < currentDay)
      } // End Try workingDay =
        // Integer.parseInt(sdf.format(dayFS.getPath().getName()));
      catch (NumberFormatException e) { /* Not a standard Day directory skip */
      }

    } // for(FileStatus dayFS : daysFS)
  }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(new ChukwaConfiguration(), DailyChukwaRecordRolling.class);

    conf.setJobName("DailyChukwa-Rolling");
    conf.setInputFormat(SequenceFileInputFormat.class);

    conf.setMapperClass(IdentityMapper.class);
    conf.setReducerClass(IdentityReducer.class);

    conf.setOutputKeyClass(ChukwaRecordKey.class);
    conf.setOutputValueClass(ChukwaRecord.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);

    log.info("DailyChukwaRecordRolling input: " + args[0]);
    log.info("DailyChukwaRecordRolling output: " + args[1]);

    FileInputFormat.setInputPaths(conf, args[0]);
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    conf.setJobPriority(JobPriority.LOW);
    conf.setNumReduceTasks(1);
    JobClient.runJob(conf);
    return 0;
  }

}
