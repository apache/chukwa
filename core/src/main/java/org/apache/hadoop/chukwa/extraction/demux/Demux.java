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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.demux.processor.ChukwaOutputCollector;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessor;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessorFactory;
import org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessorFactory;
import org.apache.hadoop.chukwa.extraction.demux.processor.reducer.ReduceProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Demux extends Configured implements Tool {
  static Logger log = Logger.getLogger(Demux.class);
  public static Configuration jobConf = null;
  protected static void setJobConf(JobConf jobConf) {
    Demux.jobConf = jobConf;
  }

  protected Configuration getJobConf() {
    return Demux.jobConf;
  }

  public static class MapClass extends MapReduceBase implements
          Mapper<ChukwaArchiveKey, ChunkImpl, ChukwaRecordKey, ChukwaRecord> {

    private Configuration jobConf = null;

    @Override
    public void configure(JobConf jobConf) {
      super.configure(jobConf);
      setJobConf(jobConf);
    }

    private void setJobConf(JobConf jobConf) {
      this.jobConf = jobConf;
    }

    public void map(ChukwaArchiveKey key, ChunkImpl chunk,
                    OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
            throws IOException {

      ChukwaOutputCollector chukwaOutputCollector = new ChukwaOutputCollector(
              "DemuxMapOutput", output, reporter);
      try {
        long duration = System.currentTimeMillis();
        if (log.isDebugEnabled()) {
          log.debug("Entry: [" + String.valueOf(chunk.getData()) + "] EventType: ["
                  + chunk.getDataType() + "]");
        }

        String defaultProcessor = jobConf.get(
                "chukwa.demux.mapper.default.processor",
                "org.apache.hadoop.chukwa.extraction.demux.processor.mapper.DefaultProcessor");

        String processorClass_pri = jobConf.get(chunk.getDataType(),
                defaultProcessor);

        String processorClass = processorClass_pri.split(",")[0];
        if (!processorClass.equalsIgnoreCase("Drop")) {
          reporter.incrCounter("DemuxMapInput", "total chunks", 1);
          reporter.incrCounter("DemuxMapInput",
                  chunk.getDataType() + " chunks", 1);

          MapProcessor processor = MapProcessorFactory
                  .getProcessor(processorClass);
          processor.process(key, chunk, chukwaOutputCollector, reporter);
          if (log.isDebugEnabled()) {
            duration = System.currentTimeMillis() - duration;
            log.debug("Demux:Map dataType:" + chunk.getDataType()
                    + " duration:" + duration + " processor:" + processorClass
                    + " recordCount:" + chunk.getRecordOffsets().length);
          }

        } else {
          log.info("action:Demux, dataType:" + chunk.getDataType()
                  + " duration:0 processor:Drop recordCount:"
                  + chunk.getRecordOffsets().length);
        }

      } catch (Exception e) {
        log.warn("Exception in Demux:MAP", e);
        e.printStackTrace();
      }
    }
  }

  public static class ReduceClass extends MapReduceBase implements
          Reducer<ChukwaRecordKey, ChukwaRecord, ChukwaRecordKey, ChukwaRecord> {

    private Configuration jobConf = null;

    public void configure(JobConf jobConf) {
      super.configure(jobConf);
      this.jobConf = jobConf;
    }

    public void reduce(ChukwaRecordKey key, Iterator<ChukwaRecord> values,
                       OutputCollector<ChukwaRecordKey, ChukwaRecord> output, Reporter reporter)
            throws IOException {
      ChukwaOutputCollector chukwaOutputCollector = new ChukwaOutputCollector(
              "DemuxReduceOutput", output, reporter);
      try {
        long duration = System.currentTimeMillis();
        reporter.incrCounter("DemuxReduceInput", "total distinct keys", 1);
        reporter.incrCounter("DemuxReduceInput", key.getReduceType()
                + " total distinct keys", 1);

        String defaultProcessor_classname = "org.apache.hadoop.chukwa.extraction.demux.processor.reducer" +
                ".IdentityReducer";
        String defaultProcessor = jobConf.get("chukwa.demux.reducer.default.processor",
                "," + defaultProcessor_classname);

        String processClass_pri = jobConf.get(key.getReduceType(), defaultProcessor);
        String[] processClass_tmps = processClass_pri.split(",");
        String processClass = null;
        if (processClass_tmps.length != 2)
          processClass = defaultProcessor_classname;
        else
          processClass = processClass_tmps[1];

        ReduceProcessor processor = ReduceProcessorFactory.getProcessor(processClass);
        System.out.println(processor.getClass().getName());
        processor.process(key, values, chukwaOutputCollector, reporter);

        if (log.isDebugEnabled()) {
          duration = System.currentTimeMillis() - duration;
          log.debug("Demux:Reduce, dataType:" + key.getReduceType()
                  + " duration:" + duration);
        }

      } catch (Exception e) {
        log.warn("Exception in Demux:Reduce", e);
        e.printStackTrace();
      }
    }
  }

  static int printUsage() {
    System.out.println("Demux [-m <maps>] [-r <reduces>] <input> <output>");
    ToolRunner.printGenericCommandUsage(System.out);
    return -1;
  }

  public static void addParsers(Configuration conf) {
    String parserPath = conf.get("chukwa.data.dir") + File.separator + "demux";
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      FileStatus[] fstatus = fs.listStatus(new Path(parserPath));
      if (fstatus != null) {
        String hdfsUrlPrefix = conf.get("fs.defaultFS");

        for (FileStatus parser : fstatus) {
          Path jarPath = new Path(parser.getPath().toString().replace(hdfsUrlPrefix, ""));
          log.debug("Adding parser JAR path " + jarPath);
          DistributedCache.addFileToClassPath(jarPath, conf);
        }
      }
    } catch (IOException e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
  }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(new ChukwaConfiguration(), Demux.class);

    SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd_HH_mm");
    conf.setJobName("Chukwa-Demux_" + day.format(new Date()));
    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setMapperClass(Demux.MapClass.class);
    conf.setPartitionerClass(ChukwaRecordPartitioner.class);
    conf.setReducerClass(Demux.ReduceClass.class);

    conf.setOutputKeyClass(ChukwaRecordKey.class);
    conf.setOutputValueClass(ChukwaRecord.class);
    conf.setOutputFormat(ChukwaRecordOutputFormat.class);
    conf.setJobPriority(JobPriority.VERY_HIGH);
    addParsers(conf);

    List<String> other_args = new ArrayList<String>();
    for (int i = 0; i < args.length; ++i) {
      try {
        if ("-m".equals(args[i])) {
          conf.setNumMapTasks(Integer.parseInt(args[++i]));
        } else if ("-r".equals(args[i])) {
          conf.setNumReduceTasks(Integer.parseInt(args[++i]));
        } else {
          other_args.add(args[i]);
        }
      } catch (NumberFormatException except) {
        System.out.println("ERROR: Integer expected instead of " + args[i]);
        return printUsage();
      } catch (ArrayIndexOutOfBoundsException except) {
        System.out.println("ERROR: Required parameter missing from "
                + args[i - 1]);
        return printUsage();
      }
    }
    // Make sure there are exactly 2 parameters left.
    if (other_args.size() != 2) {
      System.out.println("ERROR: Wrong number of parameters: "
              + other_args.size() + " instead of 2.");
      return printUsage();
    }

    FileInputFormat.setInputPaths(conf, other_args.get(0));
    FileOutputFormat.setOutputPath(conf, new Path(other_args.get(1)));

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new Demux(), args);
    return;
  }

}
