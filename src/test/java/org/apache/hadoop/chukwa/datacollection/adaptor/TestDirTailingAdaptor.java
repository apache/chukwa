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

package org.apache.hadoop.chukwa.datacollection.adaptor;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.chukwa.datacollection.agent.ChukwaAgent;
import org.apache.hadoop.conf.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import junit.framework.TestCase;

public class TestDirTailingAdaptor extends TestCase {

  ChukwaAgent agent;
  File baseDir;
  static final int SCAN_INTERVAL = 1000;
  
  /**
   * This test is exactly the same as testDirTailed except that it applies filtering and<br/>
   * creates a file that should not be read inorder to test the filter.
   * @throws IOException
   * @throws ChukwaAgent.AlreadyRunningException
   * @throws InterruptedException
   */
  public void testDirTailerFiltering() throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    
    DirTailingAdaptor.log.setLevel(Level.DEBUG);
    
    Configuration conf = new Configuration();
    baseDir = new File(System.getProperty("test.build.data", "/tmp")).getCanonicalFile();
    File checkpointDir = new File(baseDir, "dirtailerTestCheckpoints");
    createEmptyDir(checkpointDir);
    
    conf.setInt("adaptor.dirscan.intervalMs", SCAN_INTERVAL);
    conf.set("chukwaAgent.checkpoint.dir", checkpointDir.getCanonicalPath());
    conf.set("chukwaAgent.checkpoint.name", "checkpoint_");
    conf.setInt("chukwaAgent.control.port", 0);
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    
    agent = new ChukwaAgent(conf);
    File emptyDir = new File(baseDir, "emptyDir2");
    createEmptyDir(emptyDir);
    
    assertEquals(0, agent.adaptorCount());
    //check filtering with empty directory
    agent.processAddCommand("add emptydir2= DirTailingAdaptor raw " + emptyDir + " *file filetailer.CharFileTailingAdaptorUTF8 0");
    assertEquals(1, agent.adaptorCount());

    File dirWithFile = new File(baseDir, "dir3");
    dirWithFile.delete();
    assertFalse("temp directory not empty",dirWithFile.exists());
      
    //this file should be found by the filter
    dirWithFile.mkdir();
    File inDir = File.createTempFile("atemp", "file", dirWithFile);
    inDir.deleteOnExit();
    //This file should not be found by the filter
    File noreadFile = File.createTempFile("atemp", "noread", dirWithFile);
    noreadFile.deleteOnExit();
    
    //apply filter *file
    agent.processAddCommand("add dir3= DirTailingAdaptor raw " + dirWithFile + " *file filetailer.CharFileTailingAdaptorUTF8 0");
    Thread.sleep(3000);
    assertEquals(3, agent.adaptorCount());
    agent.shutdown();

    conf.setBoolean("chukwaAgent.checkpoint.enabled", true);
    Thread.sleep(1500); //wait a little bit to make sure new file ts is > last checkpoint time.
    File anOldFile = File.createTempFile("oldXYZ","file", dirWithFile);
    File aNewFile = File.createTempFile("new", "file", dirWithFile);
    anOldFile.deleteOnExit();
    aNewFile.deleteOnExit();
    anOldFile.setLastModified(10);//just after epoch
    agent = new ChukwaAgent(conf); //restart agent.
    
   Thread.sleep(3 * SCAN_INTERVAL); //wait a bit for the new file to be detected.
   assertTrue(aNewFile.exists());
   
    //make sure we started tailing the new, not the old, file.
    for(Map.Entry<String, String> adaptors : agent.getAdaptorList().entrySet()) {
      System.out.println(adaptors.getKey() +": " + adaptors.getValue());
      assertFalse(adaptors.getValue().contains("oldXYZ"));
    }
    Thread.sleep(3 * SCAN_INTERVAL); //wait a bit for the new file to be detected.
    //should be four adaptors: the DirTailer on emptyDir, the DirTailer on the full dir,
    //and FileTailers for File inDir and file newfile and not the noread file.
    assertEquals(4, agent.adaptorCount());
    agent.shutdown();
    Thread.sleep(1500); //wait a little bit to make sure new file ts is > last checkpoint time.
    
    nukeDirContents(checkpointDir);//nuke dir
    checkpointDir.delete();
    emptyDir.delete();
    nukeDirContents(dirWithFile);
    dirWithFile.delete();
  }

  
  public void testDirTailer() throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    
    DirTailingAdaptor.log.setLevel(Level.DEBUG);
    
    Configuration conf = new Configuration();
    baseDir = new File(System.getProperty("test.build.data", "/tmp")).getCanonicalFile();
    File checkpointDir = new File(baseDir, "dirtailerTestCheckpoints");
    createEmptyDir(checkpointDir);
    
    conf.setInt("adaptor.dirscan.intervalMs", SCAN_INTERVAL);
    conf.set("chukwaAgent.checkpoint.dir", checkpointDir.getCanonicalPath());
    conf.set("chukwaAgent.checkpoint.name", "checkpoint_");
    conf.setInt("chukwaAgent.control.port", 0);
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
   
    boolean retry = true; 
    while(retry) {
      try {
        retry = false;
        agent = new ChukwaAgent(conf);
      } catch(Exception e) {
        retry = true;
      }
    }
    File emptyDir = new File(baseDir, "emptyDir");
    createEmptyDir(emptyDir);
    
    assertEquals(0, agent.adaptorCount());
    agent.processAddCommand("add emptydir= DirTailingAdaptor raw " + emptyDir + " filetailer.CharFileTailingAdaptorUTF8 0");
    assertEquals(1, agent.adaptorCount());

    File dirWithFile = new File(baseDir, "dir2");
    dirWithFile.delete();
    assertFalse("temp directory not empty",dirWithFile.exists());
      
    dirWithFile.mkdir();
    File inDir = File.createTempFile("atemp", "file", dirWithFile);
    inDir.deleteOnExit();
    agent.processAddCommand("add dir2= DirTailingAdaptor raw " + dirWithFile + " *file filetailer.CharFileTailingAdaptorUTF8 0");
    Thread.sleep(3000);
    assertEquals(3, agent.adaptorCount());
    System.out.println("DirTailingAdaptor looks OK before restart");
    agent.shutdown();

    conf.setBoolean("chukwaAgent.checkpoint.enabled", true);
    Thread.sleep(1500); //wait a little bit to make sure new file ts is > last checkpoint time.
    File anOldFile = File.createTempFile("oldXYZ","file", dirWithFile);
    File aNewFile = File.createTempFile("new", "file", dirWithFile);
    anOldFile.deleteOnExit();
    aNewFile.deleteOnExit();
    anOldFile.setLastModified(10);//just after epoch
    agent = new ChukwaAgent(conf); //restart agent.
    
   Thread.sleep(3 * SCAN_INTERVAL); //wait a bit for the new file to be detected.
   assertTrue(aNewFile.exists());
   
    //make sure we started tailing the new, not the old, file.
    for(Map.Entry<String, String> adaptors : agent.getAdaptorList().entrySet()) {
      System.out.println(adaptors.getKey() +": " + adaptors.getValue());
      assertFalse(adaptors.getValue().contains("oldXYZ"));
    }
    //should be four adaptors: the DirTailer on emptyDir, the DirTailer on the full dir,
    //and FileTailers for File inDir and file newfile
    Thread.sleep(3 * SCAN_INTERVAL); //wait a bit for the new file to be detected.
    assertEquals(4, agent.adaptorCount());
    agent.shutdown();
    
    nukeDirContents(checkpointDir);//nuke dir
    checkpointDir.delete();
    emptyDir.delete();
    nukeDirContents(dirWithFile);
    dirWithFile.delete();
  }

  //returns true if dir exists
  public static boolean nukeDirContents(File dir) {
    if(dir.exists()) {
      if(dir.isDirectory()) {
        for(File f: dir.listFiles()) {
          nukeDirContents(f);
          f.delete();
        }
      } else
        dir.delete();
      
      return true;
    }
    return false;
  }
  
  public static void createEmptyDir(File dir) {
    if(!nukeDirContents(dir))
      dir.mkdir();
    assertTrue(dir.isDirectory() && dir.listFiles().length == 0);
  }

}
