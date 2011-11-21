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
package org.apache.hadoop.chukwa.analysis.salsa.fsm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.database.TableCreator;

import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent.AlreadyRunningException;
import org.apache.hadoop.chukwa.datacollection.connector.http.HttpConnector;
import org.apache.hadoop.chukwa.datacollection.collector.CaptureWriter;
import org.apache.hadoop.chukwa.datacollection.collector.servlet.ServletCollector;
import org.apache.hadoop.chukwa.datacollection.controller.ChukwaAgentController;
import org.apache.hadoop.chukwa.datacollection.sender.ChukwaHttpSender;
import org.apache.hadoop.chukwa.datacollection.sender.RetryListOfCollectors;
import org.apache.hadoop.chukwa.datacollection.test.ConsoleOutConnector;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineStageWriter;
import org.apache.hadoop.chukwa.dataloader.MetricDataLoader;
import org.apache.hadoop.conf.Configuration;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.chukwa.extraction.demux.ChukwaRecordOutputFormat;
import org.apache.hadoop.chukwa.extraction.demux.ChukwaRecordPartitioner;
import org.apache.hadoop.chukwa.extraction.demux.Demux;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.DatabaseWriter;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.analysis.salsa.fsm.*;

import junit.framework.TestCase;

public class TestFSMBuilder extends TestCase {
  private static Log log = LogFactory.getLog(TestFSMBuilder.class);

  int LINES = 10000;
  int THREADS = 2;
  private MiniDFSCluster dfs = null;
  int NUM_HADOOP_SLAVES = 4;
  private FileSystem fileSys = null;
  private MiniMRCluster mr = null;
  private Server jettyCollector = null;
  private ChukwaAgent agent = null;
  private HttpConnector conn = null;
  private ChukwaHttpSender sender = null;
  private int agentPort = 9093;
  private int collectorPort = 9990;
  private static final String dataSink = "/demux/input";
  private static final String fsmSink = "/analysis/salsafsm";
  private static Path DEMUX_INPUT_PATH = null;
  private static Path DEMUX_OUTPUT_PATH = null;
  private static Path FSM_OUTPUT_PATH = null;
  private ChukwaConfiguration conf = new ChukwaConfiguration();
  private static SimpleDateFormat day = new java.text.SimpleDateFormat("yyyyMMdd_HH_mm");
  private static String cluster = "demo";
  long[] timeWindow = {7, 30, 91, 365, 3650};
  long current = 1244617200000L;  // 2009-06-10

  public void setUp() {
    // Startup HDFS cluster - stored collector-ed JobHistory chunks
    // Startup MR cluster - run Demux, FSMBuilder
    // Startup collector
    // Startup agent
    
    System.out.println("In setUp()");
    try {
      System.setProperty("hadoop.log.dir", System.getProperty(
          "test.build.data", "/tmp"));
    } catch (Exception e) {
      e.printStackTrace();
      fail("Could not set up: " + e.toString());
    }  

    // Startup HDFS cluster - stored collector-ed JobHistory chunks
    try {
      dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true, null);
      fileSys = dfs.getFileSystem();
      DEMUX_INPUT_PATH = new Path(fileSys.getUri().toString()+File.separator+dataSink);          
      DEMUX_OUTPUT_PATH = new Path(fileSys.getUri().toString()+File.separator+"/demux/output");
    } catch(Exception e) {
      e.printStackTrace();
      fail("Fail to startup HDFS cluster.");      
    }
    // Startup MR Cluster
    try {
      mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri()
          .toString(), 1);
    } catch(Exception e) {
      fail("Fail to startup Map/reduce cluster.");
    }
    // Startup collector
    try {
      // Configure Collector
      conf.set("chukwaCollector.chunkSuppressBufferSize", "10");
      conf.set("writer.hdfs.filesystem",fileSys.getUri().toString());
      conf.set("chukwaCollector.outputDir",dataSink);
      conf.set("chukwaCollector.rotateInterval", "10000");
      
      // Set up jetty connector
      SelectChannelConnector jettyConnector = new SelectChannelConnector();
      jettyConnector.setLowResourcesConnections(THREADS-1);
      jettyConnector.setLowResourceMaxIdleTime(1500);
      jettyConnector.setPort(collectorPort);
      
      // Set up jetty server proper, using connector
      jettyCollector = new Server(collectorPort);
      Context root = new Context(jettyCollector, "/", Context.SESSIONS);
      root.addServlet(new ServletHolder(new ServletCollector(conf)), "/*");
      jettyCollector.start();
      jettyCollector.setStopAtShutdown(true);
      Thread.sleep(10000);
    } catch(Exception e) {
      fail("Fail to startup collector.");
    }

    // Startup agent
    try {
      // Configure Agent
      conf.set("chukwaAgent.tags", "cluster=\"demo\"");
      DataFactory.getInstance().addDefaultTag(conf.get("chukwaAgent.tags", "cluster=\"unknown\""));
      conf.set("chukwaAgent.checkpoint.dir", System.getenv("CHUKWA_DATA_DIR")+File.separator+"tmp");
      conf.set("chukwaAgent.checkpoint.interval", "10000");
      int portno = conf.getInt("chukwaAgent.control.port", agentPort);
      agent = new ChukwaAgent(conf);
      conn = new HttpConnector(agent, "http://localhost:"+collectorPort+"/chukwa");
      conn.start();      
      sender = new ChukwaHttpSender(conf);
      ArrayList<String> collectorList = new ArrayList<String>();
      collectorList.add("http://localhost:"+collectorPort+"/chukwa");
      sender.setCollectors(new RetryListOfCollectors(collectorList, conf));
    } catch (AlreadyRunningException e) {
      fail("Chukwa Agent is already running");
    }
    System.out.println("Done setUp().");
  }

  public String readFile(File aFile) {
    StringBuffer contents = new StringBuffer();
    try {
      BufferedReader input = new BufferedReader(new FileReader(aFile));
      try {
        String line = null; // not declared within while loop
        while ((line = input.readLine()) != null) {
          contents.append(line);
          contents.append(System.getProperty("line.separator"));
        }
      } finally {
        input.close();
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return contents.toString();
  }

  public void tearDown() {
    FileSystem fs;
    System.out.println("In tearDown()");
    try {      

      fs = dfs.getFileSystem();
      fs.delete(DEMUX_OUTPUT_PATH, true);

      agent.shutdown();
      conn.shutdown();
      jettyCollector.stop();
      mr.shutdown();
      dfs.shutdown();
      Thread.sleep(2000);
    } catch(Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    System.out.println("Done tearDown()");
  }

  /**
   * Performs tasks common to all tests
   * Sets up agent to collect samples of the 2 log types in use
   * (job history logs via JobLog and clienttrace via ClientTrace log types)
   * Calls Demux to process the logs
   */
  protected void initialTasks () {
    System.out.println("In initialTasks()");
    try {
      // Test Chukwa Agent Controller and Agent Communication
      ChukwaAgentController cli = new ChukwaAgentController("localhost", agentPort);
      String[] source = new File(System.getenv("CHUKWA_DATA_DIR") + File.separator + "log").list(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".log");
        }
      });
      System.out.println(System.getenv("CHUKWA_DATA_DIR") + File.separator + "log");
      for(String fname : source) {
        if (!(fname.equals("JobHistory.log") || fname.equals("ClientTrace.log"))) {
          continue;
        }
        StringBuilder fullPath = new StringBuilder();
        fullPath.append(System.getProperty("CHUKWA_DATA_DIR"));
        fullPath.append(File.separator);
        fullPath.append("log");
        fullPath.append(File.separator);        
        fullPath.append(fname);
        String recordType = fname.substring(0,fname.indexOf("."));
        String adaptorId = cli.add(
          "org.apache.hadoop.chukwa.datacollection.adaptor.filetailer.CharFileTailingAdaptorUTF8NewLineEscaped", 
          recordType, "0 " + fullPath.toString(), 0);
        assertNotNull(adaptorId);
        Thread.sleep(2000);
      }
      cli.removeAll();
      Thread.sleep(30000);
    } catch (Exception e) {
      e.printStackTrace();
      fail(e.toString());
    }
    
    // Test Data Sink files written by Collector    
    Path demuxDir = new Path(dataSink+"/*");
    FileSystem fs;
    try {
      fs = dfs.getFileSystem();
      FileStatus[] events = fs.globStatus(demuxDir);
      log.info("Number of data sink files written:"+events.length);
      assertTrue(events.length!=0);
    } catch (IOException e) {
      e.printStackTrace();
      fail("File System Error.");
    }
    
    // Test Demux    
    log.info("Testing demux");
    try {
      //ChukwaConfiguration conf = new ChukwaConfiguration();
      System.setProperty("hadoop.log.dir", System.getProperty(
          "test.build.data", "/tmp"));
    
      String[] sortArgs = { DEMUX_INPUT_PATH.toString(), DEMUX_OUTPUT_PATH.toString() };
      //      JobConf job = mr.createJobConf();
      JobConf job = new JobConf(new ChukwaConfiguration(), Demux.class);
      job.addResource(System.getenv("CHUKWA_CONF_DIR")+File.separator+"chukwa-demux-conf.xml");
      job.setJobName("Chukwa-Demux_" + day.format(new Date()));
      job.setInputFormat(SequenceFileInputFormat.class);
      job.setMapperClass(Demux.MapClass.class);
      job.setPartitionerClass(ChukwaRecordPartitioner.class);
      job.setReducerClass(Demux.ReduceClass.class);

      job.setOutputKeyClass(ChukwaRecordKey.class);
      job.setOutputValueClass(ChukwaRecord.class);
      job.setOutputFormat(ChukwaRecordOutputFormat.class);
      job.setJobPriority(JobPriority.VERY_HIGH);
      job.setNumMapTasks(2);
      job.setNumReduceTasks(1);
      Path input = new Path(fileSys.getUri().toString()+File.separator+dataSink+File.separator+"*.done");
      FileInputFormat.setInputPaths(job, input);
      FileOutputFormat.setOutputPath(job, DEMUX_OUTPUT_PATH);
      String[] jars = new File(System.getenv("CHUKWA_HOME")).list(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".jar");
        }
      });
      job.setJar(System.getenv("CHUKWA_HOME")+File.separator+jars[0]);
      //assertEquals(ToolRunner.run(job, new Demux(), sortArgs), 0);
      JobClient.runJob(job);
    } catch (Exception e) {
      fail(ExceptionUtil.getStackTrace(e));
    }

    System.out.println("Done initialTasks()");
  }

  public void testFSMBuilder_JobHistory020 () {
    initialTasks();
    // Test FSMBuilder (job history only)
    log.info("Testing FSMBuilder (Job History only)");
    System.out.println("In JobHistory020");
    // Run FSMBuilder on Demux output
    try {
      JobConf job = new JobConf(new ChukwaConfiguration(), FSMBuilder.class);
      job.addResource(System.getenv("CHUKWA_CONF_DIR")+File.separator+"chukwa-demux-conf.xml");
      job.setJobName("Chukwa-FSMBuilder_" + day.format(new Date()));
      job.setMapperClass(JobHistoryTaskDataMapper.class);
      job.setPartitionerClass(FSMIntermedEntryPartitioner.class);
      job.setReducerClass(FSMBuilder.FSMReducer.class);
      job.setMapOutputValueClass(FSMIntermedEntry.class);
      job.setMapOutputKeyClass(ChukwaRecordKey.class);

      job.setInputFormat(SequenceFileInputFormat.class);
      job.setOutputKeyClass(ChukwaRecordKey.class);
      job.setOutputValueClass(ChukwaRecord.class);
      job.setOutputFormat(ChukwaRecordOutputFormat.class);
      job.setNumReduceTasks(1);

      Path inputPath = new Path(DEMUX_OUTPUT_PATH.toString()+File.separator+"/*/*/TaskData*.evt");
      this.FSM_OUTPUT_PATH = new Path(fileSys.getUri().toString()+File.separator+fsmSink);

      FileInputFormat.setInputPaths(job, inputPath);
      FileOutputFormat.setOutputPath(job, FSM_OUTPUT_PATH);

      String[] jars = new File(System.getenv("CHUKWA_HOME")).list(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".jar");
        }
      });
      job.setJar(System.getenv("CHUKWA_HOME")+File.separator+jars[0]);
      JobClient.runJob(job);
    } catch (Exception e) {
      fail("Error running FSMBuilder: "+e.toString());
    }
    System.out.println("Done running FSMBuilder; Checking results");
  
    // Check FSMBuilder output by reading the sequence file(s) generated
    // Hard-coded to check the contents of test/samples/JobLog.log
    try {

      Pattern task_id_pat = Pattern.compile("attempt_[0-9]*_[0-9]*_[mr]_([0-9]*)_[0-9]*");

      ChukwaRecordKey key = new ChukwaRecordKey();
      ChukwaRecord record = new ChukwaRecord();

      // initialize data structures for checking FSM
      // should see 10 maps, 8 reduces
      boolean mapSeen[] = new boolean[10];
      boolean reduceSeen[] = new boolean[8];
      boolean reduceShuffleSeen[] = new boolean[8];
      boolean reduceSortSeen[] = new boolean[8];
      boolean reduceReducerSeen[] = new boolean[8];
      for (int i = 0; i < 10; i++) mapSeen[i] = false;
      for (int i = 0; i < 8; i++) { 
        reduceSeen[i] = false;
        reduceShuffleSeen[i] = false;
        reduceSortSeen[i] = false;
        reduceReducerSeen[i] = false;
      }

      Path fsm_outputs = new Path(FSM_OUTPUT_PATH.toString()+File.separator+
        "/*/MAPREDUCE_FSM/MAPREDUCE_FSM*.evt");
      FileStatus [] files;
      files = fileSys.globStatus(fsm_outputs);
      int count = 0;

      for (int i = 0; i < files.length; i++) {
        SequenceFile.Reader r = new SequenceFile.Reader(fileSys, files[i].getPath(), conf);
        System.out.println("Processing files " + files[i].getPath().toString());
        while (r.next(key, record)) {
          String state_name = record.getValue("STATE_NAME");
          String task_id = record.getValue("TASK_ID");
          
          Matcher m = task_id_pat.matcher(task_id);
          if (!m.matches()) {
            continue;
          }
          String tasknum_string = m.group(1);
          if (tasknum_string == null) {
            continue;
          }
          int tasknum = Integer.parseInt(tasknum_string);
  
          if (state_name.equals("MAP")) {
            assertTrue("Map sequence number should be < 10",tasknum < 10);
            mapSeen[tasknum] = true;
          } else if (state_name.equals("REDUCE")) {
            assertTrue("Reduce sequence number should be < 8",tasknum < 8);
            reduceSeen[tasknum] = true;
          } else if (state_name.equals("REDUCE_SHUFFLEWAIT")) {
            assertTrue("Reduce sequence number should be < 8",tasknum < 8);
            reduceShuffleSeen[tasknum] = true;
          } else if (state_name.equals("REDUCE_SORT")) {
            assertTrue("Reduce sequence number should be < 8",tasknum < 8);
            reduceSortSeen[tasknum] = true;
          } else if (state_name.equals("REDUCE_REDUCER")) {
            assertTrue("Reduce sequence number should be < 8",tasknum < 8);
            reduceReducerSeen[tasknum] = true;
          }
          count++;
        }
      }
      System.out.println("Processed " + count + " records.");
      assertTrue("Total number of states is 42 - 10 maps + (8 reduces * 4)",count == 42);  

      // We must have seen all 10 maps and all 8 reduces; 
      // check for that here
      boolean passed = true;
      for (int i = 0; i < 10; i++) passed &= mapSeen[i];
      for (int i = 0; i < 8; i++) {
        passed &= reduceSeen[i];
        passed &= reduceShuffleSeen[i];
        passed &= reduceSortSeen[i];
        passed &= reduceReducerSeen[i];
      }

      assertTrue("Seen all Maps and Reduces in generated states.",passed);

    } catch (Exception e) {
      fail("Error checking FSMBuilder output: "+e.toString());
    } 
    
  }

  public void testFSMBuilder_ClientTrace020 () {
    initialTasks();
    // Test FSMBuilder (job history only)
    log.info("Testing FSMBuilder (ClientTrace only)");
    System.out.println("In ClientTrace020");
    // Run FSMBuilder on Demux output
    try {
      // Process TaskTracker shuffle clienttrace entries first
      JobConf job = new JobConf(new ChukwaConfiguration(), FSMBuilder.class);
      job.addResource(System.getenv("CHUKWA_CONF_DIR")+File.separator+"chukwa-demux-conf.xml");
      job.setJobName("Chukwa-FSMBuilder_" + day.format(new Date()));
      job.setMapperClass(TaskTrackerClientTraceMapper.class);
      job.setPartitionerClass(FSMIntermedEntryPartitioner.class);
      job.setReducerClass(FSMBuilder.FSMReducer.class);
      job.setMapOutputValueClass(FSMIntermedEntry.class);
      job.setMapOutputKeyClass(ChukwaRecordKey.class);

      job.setInputFormat(SequenceFileInputFormat.class);
      job.setOutputKeyClass(ChukwaRecordKey.class);
      job.setOutputValueClass(ChukwaRecord.class);
      job.setOutputFormat(ChukwaRecordOutputFormat.class);
      job.setNumReduceTasks(1);

      Path inputPath = new Path(DEMUX_OUTPUT_PATH.toString()+File.separator+"/*/*/ClientTraceDetailed*.evt");
      Path fsmOutputPath1 = new Path(fileSys.getUri().toString()+File.separator+fsmSink+"1");

      FileInputFormat.setInputPaths(job, inputPath);
      FileOutputFormat.setOutputPath(job, fsmOutputPath1);

      String[] jars = new File(System.getenv("CHUKWA_HOME")).list(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".jar");
        }
      });
      job.setJar(System.getenv("CHUKWA_HOME")+File.separator+jars[0]);
      JobClient.runJob(job);
      System.out.println("Processed TaskTracker ClientTrace.");

      // Process DataNode clienttrace entries
      job = new JobConf(new ChukwaConfiguration(), FSMBuilder.class);
      job.addResource(System.getenv("CHUKWA_CONF_DIR")+File.separator+"chukwa-demux-conf.xml");
      job.setJobName("Chukwa-FSMBuilder_" + day.format(new Date()));
      job.setMapperClass(DataNodeClientTraceMapper.class);
      job.setPartitionerClass(FSMIntermedEntryPartitioner.class);
      job.setReducerClass(FSMBuilder.FSMReducer.class);
      job.setMapOutputValueClass(FSMIntermedEntry.class);
      job.setMapOutputKeyClass(ChukwaRecordKey.class);

      job.setInputFormat(SequenceFileInputFormat.class);
      job.setOutputKeyClass(ChukwaRecordKey.class);
      job.setOutputValueClass(ChukwaRecord.class);
      job.setOutputFormat(ChukwaRecordOutputFormat.class);
      job.setNumReduceTasks(1);

      inputPath = new Path(DEMUX_OUTPUT_PATH.toString()+File.separator+"/*/*/ClientTraceDetailed*.evt");
      Path fsmOutputPath2 = new Path(fileSys.getUri().toString()+File.separator+fsmSink+"2");

      FileInputFormat.setInputPaths(job, inputPath);
      FileOutputFormat.setOutputPath(job, fsmOutputPath2);

      jars = new File(System.getenv("CHUKWA_HOME")).list(new FilenameFilter() {
        public boolean accept(File dir, String name) {
          return name.endsWith(".jar");
        }
      });
      job.setJar(System.getenv("CHUKWA_HOME")+File.separator+jars[0]);
      JobClient.runJob(job);
      System.out.println("Processed DataNode ClientTrace.");

    } catch (Exception e) {
      fail("Error running FSMBuilder: "+e.toString());
    }
    System.out.println("Done running FSMBuilder; Checking results");

    try {
      Path fsm_outputs = new Path(fileSys.getUri().toString()+File.separator+
        fsmSink + "*/*/*/*.evt");
      FileStatus [] files;
      files = fileSys.globStatus(fsm_outputs);
      int count = 0;
      int numHDFSRead = 0, numHDFSWrite = 0, numShuffles = 0;
      ChukwaRecordKey key = new ChukwaRecordKey();
      ChukwaRecord record = new ChukwaRecord();

      for (int i = 0; i < files.length; i++) {
        SequenceFile.Reader r = new SequenceFile.Reader(fileSys, files[i].getPath(), conf);
        System.out.println("Processing files " + files[i].getPath().toString());
        while (r.next(key, record)) {
          String state_name = record.getValue("STATE_NAME");
  
          if (state_name.equals("READ_LOCAL") || state_name.equals("READ_REMOTE")) 
          {
            numHDFSRead++;
          } else if (state_name.equals("WRITE_LOCAL") || state_name.equals("WRITE_REMOTE") 
              || state_name.equals("WRITE_REPLICATED")) 
          {
            numHDFSWrite++;
          } else if (state_name.equals("SHUFFLE_LOCAL") || state_name.equals("SHUFFLE_REMOTE")) 
          {
            numShuffles++;
          }
          count++;
        }
      }
      System.out.println("Processed " + count + " records."); 
      System.out.println("HDFSRD: " + numHDFSRead + " HDFSWR: " + numHDFSWrite + " SHUF: " + numShuffles);
      assertTrue("Number of HDFS reads", numHDFSRead == 10);
      assertTrue("Number of HDFS writes", numHDFSWrite == 8);      
      assertTrue("Number of shuffles", numShuffles == 80);

    } catch (Exception e) {
      fail("Error checking FSMBuilder results: " + e.toString());
    }      
  }

}

