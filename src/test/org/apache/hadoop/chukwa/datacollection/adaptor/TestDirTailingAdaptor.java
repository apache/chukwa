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
import junit.framework.TestCase;

public class TestDirTailingAdaptor extends TestCase {

  ChukwaAgent agent;
  File baseDir;
  static final int SCAN_INTERVAL = 1000;

  public void testDirTailer() throws IOException,
  ChukwaAgent.AlreadyRunningException, InterruptedException {
    
    Configuration conf = new Configuration();
    baseDir = new File(System.getProperty("test.build.data", "/tmp"));
    File checkpointDir = new File(baseDir, "dirtailerTestCheckpoints");
    createEmptyDir(checkpointDir);
    
    conf.setInt("adaptor.dirscan.intervalMs", SCAN_INTERVAL);
    conf.set("chukwaAgent.checkpoint.dir", checkpointDir.getCanonicalPath());
    conf.set("chukwaAgent.checkpoint.name", "checkpoint_");
    conf.setInt("chukwaAgent.control.port", 0);
    conf.setBoolean("chukwaAgent.checkpoint.enabled", false);
    
    agent = new ChukwaAgent(conf);
    File emptyDir = new File(baseDir, "emptyDir");
    createEmptyDir(emptyDir);
    
    assertEquals(0, agent.adaptorCount());
    agent.processAddCommand("add DirTailingAdaptor raw " + emptyDir + " filetailer.CharFileTailingAdaptorUTF8 0");
    assertEquals(1, agent.adaptorCount());

    File dirWithFile = new File(baseDir, "dir2");
    dirWithFile.delete();
    assertFalse("temp directory not empty",dirWithFile.exists());
      
    dirWithFile.mkdir();
    File inDir = File.createTempFile("atemp", "file", dirWithFile);
    inDir.deleteOnExit();
    agent.processAddCommand("add DirTailingAdaptor raw " + dirWithFile + " filetailer.CharFileTailingAdaptorUTF8 0");
    Thread.sleep(3000);
    assertEquals(3, agent.adaptorCount());
    System.out.println("DirTailingAdaptor looks OK before restart");
    agent.shutdown();

    conf.setBoolean("chukwaAgent.checkpoint.enabled", true);

    File anOldFile = File.createTempFile("oldXYZ","file", dirWithFile);
    File aNewFile = File.createTempFile("new", "file", dirWithFile);
    anOldFile.deleteOnExit();
    aNewFile.deleteOnExit();
    anOldFile.setLastModified(10);//just after epoch
    agent = new ChukwaAgent(conf); //restart agent.
    
    Thread.sleep(3 * SCAN_INTERVAL); //wait a bit for the new file to be detected.
    
    //make sure we started tailing the new, not the old, file.
    for(Map.Entry<String, String> adaptors : agent.getAdaptorList().entrySet()) {
      System.out.println(adaptors.getKey() +": " + adaptors.getValue());
      assertFalse(adaptors.getValue().contains("oldXYZ"));
    }
    //should be four adaptors: the DirTailer on emptyDir, the DirTailer on the full dir,
    //and FileTailers for File inDir and file newfile
    assertEquals(4, agent.adaptorCount());
    
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
  
  private void createEmptyDir(File dir) {
    if(!nukeDirContents(dir))
      dir.mkdir();
    assertTrue(dir.isDirectory() && dir.listFiles().length == 0);
  }

}
