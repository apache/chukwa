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
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;

public class TestDemuxManager extends TestCase {

  /**
   * Standard workflow
   */
  public void testScenario1() {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String tempDirectory = System.getProperty("test.build.data", "/tmp");
    String chukwaRootDir = tempDirectory + "/demuxManagerTest_" + System.currentTimeMillis() +"/";
    
    cc.set(CHUKWA_CONSTANT.HDFS_DEFAULT_NAME_FIELD, "file:///");
    cc.set(CHUKWA_CONSTANT.CHUKWA_ROOT_DIR_FIELD, chukwaRootDir );
    cc.set(CHUKWA_CONSTANT.CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +"/archives/" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +"/postProcess" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +"/logs" );
    
     
    try {
      
      File dataSinkDirectory = new File(chukwaRootDir +"/logs");
      dataSinkDirectory.mkdirs();
      File dataSinkFile = new File(chukwaRootDir +"/logs"+ "/dataSink1.done");
      dataSinkFile.createNewFile();
      
      DemuxManagerScenario dm = new DemuxManagerScenario1(cc,0);  
      dm.start();
      
      List<String> requireActions = new ArrayList<String>();
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("checkDemuxInputDir:false");
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true");
      requireActions.add("runDemux:true");
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true");
      requireActions.add("processData done");
      
      List<String> actions = dm.actions;
     
      Assert.assertTrue(requireActions.size() == actions.size());
      
      for(int i=0;i<requireActions.size();i++) {
        Assert.assertTrue( requireActions.get(i) + " == " +actions.get(i),requireActions.get(i).intern() == actions.get(i).intern());
      }

    } catch (Exception e) {
      e.printStackTrace();
     Assert.fail();
    }
    finally {
      deleteDirectory(new File(chukwaRootDir));
    }
  }
  
  /**
   * No dataSink file at  startup
   * Add one later
   */
  public void testScenario2() {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String tempDirectory = System.getProperty("test.build.data", "/tmp");
    String chukwaRootDir = tempDirectory + "/demuxManagerTest_" + System.currentTimeMillis() +"/";
    
    cc.set(CHUKWA_CONSTANT.HDFS_DEFAULT_NAME_FIELD, "file:///");
    cc.set(CHUKWA_CONSTANT.CHUKWA_ROOT_DIR_FIELD, chukwaRootDir );
    cc.set(CHUKWA_CONSTANT.CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +"/archives/" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +"/postProcess" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +"/logs" );

    try {
      DemuxManagerScenario dm = new DemuxManagerScenario2(cc);  
      dm.start();
 
      List<String> requireActions = new ArrayList<String>();
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("checkDemuxInputDir:false");
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:false");
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("checkDemuxInputDir:false");
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true");
      requireActions.add("runDemux:true");
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true");
      requireActions.add("processData done");

      List<String> actions = dm.actions;
      Assert.assertTrue(requireActions.size() == actions.size());
      
      for(int i=0;i<requireActions.size();i++) {
        Assert.assertTrue( requireActions.get(i) + " == " +actions.get(i),requireActions.get(i).intern() == actions.get(i).intern());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    finally {
      deleteDirectory(new File(chukwaRootDir));
    }
  }
  
  
  /**
   * DataSink file in dataSink directory
   * MR_INPUT_DIR already there
   * MR_INPUT_DIR should be reprocessed first
   * Then dataSink file
   */
  public void testScenario3() {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String tempDirectory = System.getProperty("test.build.data", "/tmp");
    String chukwaRootDir = tempDirectory + "/demuxManagerTest_" + System.currentTimeMillis() +"/";
    
    cc.set(CHUKWA_CONSTANT.HDFS_DEFAULT_NAME_FIELD, "file:///");
    cc.set(CHUKWA_CONSTANT.CHUKWA_ROOT_DIR_FIELD, chukwaRootDir );
    cc.set(CHUKWA_CONSTANT.CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +"/archives/" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +"/postProcess" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +"/logs" );

    try {
      File mrInputDir = new File(chukwaRootDir + CHUKWA_CONSTANT.DEFAULT_DEMUX_PROCESSING_DIR_NAME+ CHUKWA_CONSTANT.DEFAULT_DEMUX_MR_INPUT_DIR_NAME);
      mrInputDir.mkdirs();
      File dataSinkDirectory = new File(chukwaRootDir +"/logs");
      dataSinkDirectory.mkdirs();
      File dataSinkFile = new File(chukwaRootDir +"/logs"+ "/dataSink3.done");
      dataSinkFile.createNewFile();
      
      DemuxManagerScenario dm = new DemuxManagerScenario1(cc,2);  
      dm.start();
 
      List<String> requireActions = new ArrayList<String>();
     
      // DEMUX_INPUT_DIR reprocessing
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("checkDemuxInputDir:true");
      requireActions.add("runDemux:true");
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true");
      requireActions.add("processData done");

      // dataSink3.done processing
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:false"); 
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true"); 
      requireActions.add("runDemux:true"); 
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true"); 
      requireActions.add("processData done");
      
      List<String> actions = dm.actions;
      Assert.assertTrue(requireActions.size() == actions.size());
      
      for (int i=0;i<requireActions.size();i++) {
        Assert.assertTrue( requireActions.get(i) + " == " +actions.get(i),requireActions.get(i).intern() == actions.get(i).intern());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    finally {
      deleteDirectory(new File(chukwaRootDir));
    }
  }
  
  
  /**
   * DataSink file in dataSink directory
   * MR_INPUT_DIR already there
   * MR_OUTPUT_DIR already there
   * MR_OUTPUT_DIR should be deleted first
   * Then MR_INPUT_DIR should be reprocessed
   * Then dataSink file
   */
  public void testScenario4() {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String tempDirectory = System.getProperty("test.build.data", "/tmp");
    String chukwaRootDir = tempDirectory + "/demuxManagerTest_" + System.currentTimeMillis() +"/";
    
    cc.set(CHUKWA_CONSTANT.HDFS_DEFAULT_NAME_FIELD, "file:///");
    cc.set(CHUKWA_CONSTANT.CHUKWA_ROOT_DIR_FIELD, chukwaRootDir );
    cc.set(CHUKWA_CONSTANT.CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +"/archives/" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +"/postProcess" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +"/logs" );

    try {
      File mrInputDir = new File(chukwaRootDir + CHUKWA_CONSTANT.DEFAULT_DEMUX_PROCESSING_DIR_NAME+ CHUKWA_CONSTANT.DEFAULT_DEMUX_MR_INPUT_DIR_NAME);
      mrInputDir.mkdirs();
      File mrOutputDir = new File(chukwaRootDir + CHUKWA_CONSTANT.DEFAULT_DEMUX_PROCESSING_DIR_NAME+ CHUKWA_CONSTANT.DEFAULT_DEMUX_MR_OUTPUT_DIR_NAME);
      mrOutputDir.mkdirs();
      
      File dataSinkDirectory = new File(chukwaRootDir +"/logs");
      dataSinkDirectory.mkdirs();
      File dataSinkFile = new File(chukwaRootDir +"/logs"+ "/dataSink4.done");
      dataSinkFile.createNewFile();
      
      DemuxManagerScenario dm = new DemuxManagerScenario1(cc,2);  
      dm.start();
 
      List<String> requireActions = new ArrayList<String>();

      requireActions.add("checkDemuxOutputDir:true"); 
      requireActions.add("deleteDemuxOutputDir:true");
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("checkDemuxInputDir:true");
      requireActions.add("runDemux:true"); 
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true"); 
      requireActions.add("processData done");

      // dataSink4.done processing
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:false"); 
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true"); 
      requireActions.add("runDemux:true"); 
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true"); 
      requireActions.add("processData done");
            
      List<String> actions = dm.actions;
      Assert.assertTrue(requireActions.size() == actions.size());
      
      for(int i=0;i<requireActions.size();i++) {
        Assert.assertTrue( requireActions.get(i) + " == " +actions.get(i),requireActions.get(i).intern() == actions.get(i).intern());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    finally {
      deleteDirectory(new File(chukwaRootDir));
    }
  }
  
  /**
   * DataSink file in dataSinkDir
   * Demux fails 3 times
   * Add a new DataSink file to dataSinkDir
   * Demux succeed
   */
  public void testScenario5() {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String tempDirectory = System.getProperty("test.build.data", "/tmp");
    String chukwaRootDir = tempDirectory + "/demuxManagerTest_" + System.currentTimeMillis() +"/";
    
    cc.set(CHUKWA_CONSTANT.HDFS_DEFAULT_NAME_FIELD, "file:///");
    cc.set(CHUKWA_CONSTANT.CHUKWA_ROOT_DIR_FIELD, chukwaRootDir );
    cc.set(CHUKWA_CONSTANT.CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +"/archives/" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +"/postProcess" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +"/logs" );

    try {
      File dataSinkDirectory = new File(chukwaRootDir +"/logs");
      dataSinkDirectory.mkdirs();
      File dataSinkFile = new File(chukwaRootDir +"/logs"+ "/dataSink5-0.done");
      dataSinkFile.createNewFile();
      
      DemuxManagerScenario dm = new DemuxManagerScenario5(cc);  
      dm.start();
 
      List<String> requireActions = new ArrayList<String>();
      
      // Move dataSink & process
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:false"); 
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true"); 
      requireActions.add("runDemux:false"); 
      requireActions.add("processData done"); 
      
      // Reprocess 1
      requireActions.add("checkDemuxOutputDir:true"); 
      requireActions.add("deleteDemuxOutputDir:true");
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:true"); 
      requireActions.add("runDemux:false"); 
      requireActions.add("processData done"); 
      
      // Reprocess 2
      requireActions.add("checkDemuxOutputDir:true"); 
      requireActions.add("deleteDemuxOutputDir:true");
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:true"); 
      requireActions.add("runDemux:false"); 
      requireActions.add("processData done"); 
      
      // Reprocess3
      requireActions.add("checkDemuxOutputDir:true"); 
      requireActions.add("deleteDemuxOutputDir:true");
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:true"); 
      requireActions.add("runDemux:false"); 
      requireActions.add("processData done"); 
      requireActions.add("checkDemuxOutputDir:true"); 
      requireActions.add("deleteDemuxOutputDir:true");
      requireActions.add("checkDemuxOutputDir:false");
      requireActions.add("checkDemuxInputDir:true");
      requireActions.add("moveDataSinkFilesToDemuxErrorDirectory:true");
     
      requireActions.add("checkDemuxOutputDir:false"); 
      requireActions.add("checkDemuxInputDir:false"); 
      requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true"); 
      requireActions.add("runDemux:true"); 
      requireActions.add("checkDemuxOutputDir:true");
      requireActions.add("moveDemuxOutputDirToPostProcessDirectory:true"); 
      requireActions.add("moveDataSinkFilesToArchiveDirectory:true"); 
      requireActions.add("processData done");
      
      
      List<String> actions = dm.actions;
      Assert.assertTrue(requireActions.size() == actions.size());
      
      for(int i=0;i<requireActions.size();i++) {
        Assert.assertTrue( i + " - " + requireActions.get(i) + " == " +actions.get(i),requireActions.get(i).intern() == actions.get(i).intern());
      }

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
    finally {
      deleteDirectory(new File(chukwaRootDir));
    }
  }
  
  
  /**
   * Standard workflow with MR_OUTPUT
   */
  public void testScenario6() {
    ChukwaConfiguration cc = new ChukwaConfiguration();
    String tempDirectory = System.getProperty("test.build.data", "/tmp");
    String chukwaRootDir = tempDirectory + "/demuxManagerTest_" + System.currentTimeMillis() +"/";
    
    
    cc.set(CHUKWA_CONSTANT.WRITER_HDFS_FILESYSTEM_FIELD, "file:///");
    cc.set(CHUKWA_CONSTANT.CHUKWA_ROOT_DIR_FIELD, chukwaRootDir );
    cc.set(CHUKWA_CONSTANT.CHUKWA_ARCHIVE_DIR_FIELD, chukwaRootDir +"/archives/" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_POST_PROCESS_DIR_FIELD, chukwaRootDir +"/postProcess" );
    cc.set(CHUKWA_CONSTANT.CHUKWA_DATA_SINK_DIR_FIELD, chukwaRootDir +"/logs" );
    
    try {
      
      File dataSinkDirectory = new File(chukwaRootDir +"/logs");
      dataSinkDirectory.mkdirs();
      File dataSinkFile = new File(chukwaRootDir +"/logs"+ "/dataSink6.done");
      dataSinkFile.createNewFile();
      
      DemuxManagerScenario dm = new DemuxManagerScenario6(cc,3);  
      dm.start();
      
      List<String> requireActions = new ArrayList<String>();
      for (int i=0;i<3;i++) {
        requireActions.add("checkDemuxOutputDir:false");
        requireActions.add("checkDemuxInputDir:false");
        requireActions.add("moveDataSinkFilesToDemuxInputDirectory:true");
        requireActions.add("runDemux:true");
        requireActions.add("checkDemuxOutputDir:true");
        requireActions.add("moveDemuxOutputDirToPostProcessDirectory:true");
        requireActions.add("moveDataSinkFilesToArchiveDirectory:true");
        requireActions.add("processData done");   
      }
     
      List<String> actions = dm.actions;
      Assert.assertTrue(requireActions.size() == actions.size());
      
      for(int i=0;i<requireActions.size();i++) {
        Assert.assertTrue( requireActions.get(i) + " == " +actions.get(i),requireActions.get(i).intern() == actions.get(i).intern());
      }

      
    } catch (Exception e) {
      e.printStackTrace();
     Assert.fail();
    }
    finally {
      deleteDirectory(new File(chukwaRootDir));
    }
    
  }
  
  
  static public boolean deleteDirectory(File path) {
    if( path.exists() ) {
      File[] files = path.listFiles();
      for(int i=0; i<files.length; i++) {
         if(files[i].isDirectory()) {
           deleteDirectory(files[i]);
         }
         else {
           files[i].delete();
         }
      }
    }
    return( path.delete() );
  }

     /////////////////////////\
    //// HELPER CLASSES /////  \
   /////////////////////////____\
  
  private static class DemuxManagerScenario6 extends DemuxManagerScenario {
    int count = 0;
    public DemuxManagerScenario6(ChukwaConfiguration conf, int count) throws Exception {
      super(conf);
      this.count = count;
    }
    @Override
    public boolean runDemux(String demuxInputDir, String demuxOutputDir) {
      boolean res = super.runDemux(demuxInputDir, demuxOutputDir);
      
      try {
        // Create DEMUX_OUTPOUT
        String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD);
        File mrOutputDir = new File(chukwaRootDir  + DEFAULT_DEMUX_PROCESSING_DIR_NAME+ DEFAULT_DEMUX_MR_OUTPUT_DIR_NAME);
        mrOutputDir.mkdirs();
        mrOutputDir.deleteOnExit();
        
        // ADD DATASINK FILE
        File dataSinkDirectory = new File(chukwaRootDir +"logs");
        dataSinkDirectory.mkdirs();
        File dataSinkFile = new File(chukwaRootDir +"logs"+ "/dataSink6-" + count + ".done");
        dataSinkFile.createNewFile();
        
      }catch(Exception e) {
        throw new RuntimeException(e);
      }

      count --;
      if (count <= 0) {
        this.isRunning = false;
      }
     
      return res;
    }
  }
  
  
  private static class DemuxManagerScenario5 extends DemuxManagerScenario {
    public DemuxManagerScenario5(ChukwaConfiguration conf) throws Exception {
      super(conf);
    }
    
    boolean errorDone = false;
    public boolean moveDataSinkFilesToDemuxErrorDirectory(String dataSinkDir,
        String demuxErrorDir) throws IOException {
      boolean res = super.moveDataSinkFilesToDemuxErrorDirectory(dataSinkDir, demuxErrorDir);
      
      String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD);
      File dataSinkDirectory = new File(chukwaRootDir +"logs");
      dataSinkDirectory.mkdirs();
      dataSinkDirectory.deleteOnExit();
      File dataSinkFile = new File(chukwaRootDir +"logs"+ "/dataSink5-1.done");
      dataSinkFile.createNewFile();
      dataSinkFile.deleteOnExit();       
  
      errorDone = true;
      return res;
    }
    
    int counter = 0;
    @Override
    public boolean runDemux(String demuxInputDir, String demuxOutputDir) {
      if (errorDone && counter >= 4) {
        this.isRunning = false;
      } 
      
      // Create DEMUX_OUTPOUT
      String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD);
      File mrOutputDir = new File(chukwaRootDir  + DEFAULT_DEMUX_PROCESSING_DIR_NAME+ DEFAULT_DEMUX_MR_OUTPUT_DIR_NAME);
      mrOutputDir.mkdirs();
      mrOutputDir.deleteOnExit();
      
      counter ++;
      this.actions.add("runDemux:" + errorDone);
      return errorDone;
    }
  }
  
  private static class DemuxManagerScenario2 extends DemuxManagerScenario {
    public DemuxManagerScenario2(ChukwaConfiguration conf) throws Exception {
      super(conf);
    }
    
    int counter = 0;
    @Override
    public boolean moveDataSinkFilesToDemuxInputDirectory(String dataSinkDir,
        String demuxInputDir) throws IOException {
      if (counter == 1) {
        String chukwaRootDir = conf.get(CHUKWA_ROOT_DIR_FIELD);
        File dataSinkDirectory = new File(chukwaRootDir +"logs");
        dataSinkDirectory.mkdirs();
        dataSinkDirectory.deleteOnExit();
        File dataSinkFile = new File(chukwaRootDir +"logs"+ "/dataSink2.done");
        dataSinkFile.createNewFile();
        dataSinkFile.deleteOnExit();
      }

      counter ++;
      return super.moveDataSinkFilesToDemuxInputDirectory(dataSinkDir, demuxInputDir);
    }
    
   
    @Override
    public boolean runDemux(String demuxInputDir, String demuxOutputDir) {
      boolean res = super.runDemux(demuxInputDir, demuxOutputDir);
      if (counter > 1) {
        this.isRunning = false;
      }
      return res;
    }
  }
  
  private static class DemuxManagerScenario1 extends DemuxManagerScenario {
    int count = 0;
    public DemuxManagerScenario1(ChukwaConfiguration conf, int count) throws Exception {
      super(conf);
      this.count = count;
    }
    @Override
    public boolean runDemux(String demuxInputDir, String demuxOutputDir) {
      boolean res = super.runDemux(demuxInputDir, demuxOutputDir);
      count --;
      if (count <= 0) {
        this.isRunning = false;
      }
     
      return res;
    }
  }
  
  private static class DemuxManagerScenario extends DemuxManager {
    public List<String>actions = new ArrayList<String>();
 
    public DemuxManagerScenario(ChukwaConfiguration conf) throws Exception {
      super(conf);
      NO_DATASINK_SLEEP_TIME = 5;
    }

    @Override
    public boolean checkDemuxInputDir(String demuxInputDir) throws IOException {
      boolean res = super.checkDemuxInputDir(demuxInputDir);
      this.actions.add("checkDemuxInputDir:" + res);
      return res;
    }

    @Override
    public boolean checkDemuxOutputDir(String demuxOutputDir)
        throws IOException {
      boolean res = super.checkDemuxOutputDir(demuxOutputDir);
      this.actions.add("checkDemuxOutputDir:" + res);
      return res;
    }

    @Override
    public boolean moveDataSinkFilesToArchiveDirectory(String demuxInputDir,
        String archiveDirectory) throws IOException {
      boolean res = super.moveDataSinkFilesToArchiveDirectory(demuxInputDir,
          archiveDirectory);
      this.actions.add("moveDataSinkFilesToArchiveDirectory:" + res);
      return res;
    }

    @Override
    public boolean moveDataSinkFilesToDemuxErrorDirectory(String dataSinkDir,
        String demuxErrorDir) throws IOException {
      boolean res = super.moveDataSinkFilesToDemuxErrorDirectory(dataSinkDir, demuxErrorDir);
      this.actions.add("moveDataSinkFilesToDemuxErrorDirectory:" + res);
      return res;
    }

    @Override
    public boolean moveDataSinkFilesToDemuxInputDirectory(String dataSinkDir,
        String demuxInputDir) throws IOException {
      boolean res = super.moveDataSinkFilesToDemuxInputDirectory(dataSinkDir, demuxInputDir);
      this.actions.add("moveDataSinkFilesToDemuxInputDirectory:" + res);
      return res;
    }

    @Override
    public boolean moveDemuxOutputDirToPostProcessDirectory(
        String demuxOutputDir, String postProcessDirectory) throws IOException {
      boolean res = super.moveDemuxOutputDirToPostProcessDirectory(demuxOutputDir,
          postProcessDirectory);
      this.actions.add("moveDemuxOutputDirToPostProcessDirectory:" + res);
      return res;
    }

    @Override
    public boolean processData(String dataSinkDir, String demuxInputDir,
        String demuxOutputDir, String postProcessDir, String archiveDir)
        throws IOException {
      boolean res = super.processData(dataSinkDir, demuxInputDir, demuxOutputDir, postProcessDir,
          archiveDir);
      this.actions.add("processData done");
      return res;
    }

    @Override
    public boolean runDemux(String demuxInputDir, String demuxOutputDir) {
      boolean res = true;
      this.actions.add("runDemux:" + res);
      return res;
    }

    @Override
    public boolean deleteDemuxOutputDir(String demuxOutputDir)
        throws IOException {
      boolean res = super.deleteDemuxOutputDir(demuxOutputDir);
      this.actions.add("deleteDemuxOutputDir:" + res);
      return res;
    }
    
  }

}
