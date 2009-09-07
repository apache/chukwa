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

import java.io.IOException;
import java.util.Calendar;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.extraction.CHUKWA_CONSTANT;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.ToolRunner;
import junit.framework.TestCase;
import static org.apache.hadoop.chukwa.util.TempFileUtil.writeASinkFile;

public class TestArchive extends TestCase {

  
   public void browseDir(FileSystem fs, Path p, int d) throws IOException {
     for(int i=0; i< d; ++i) {
       System.out.print(" |");
     }
     FileStatus stat = fs.getFileStatus(p);
     if(stat.isDir()) {
       System.out.println(" \\ " + p.getName());
       FileStatus[] files = fs.listStatus(p);
       for(FileStatus f: files) {
         browseDir(fs, f.getPath(), d+1);
       }
     }
     else
       System.out.println( p.getName() );
   }

  static final int NUM_HADOOP_SLAVES = 1;
  static final Path DATASINK = new Path("/chukwa/logs/*");
  static final Path DATASINKFILE = new Path("/chukwa/logs/foobar.done");
  static final Path DATASINK_NOTDONE = new Path("/chukwa/logs/foo.chukwa");
  static final Path DEST_FILE = new Path("/chukwa/archive/foocluster/HadoopLogProcessor_2008_05_29.arc");
  static final Path MERGED_DATASINK = new Path("/chukwa/archive/foocluster/HadoopLogProcessor_2008_05_29-0.arc");
  static final Path OUTPUT_DIR = new Path("/chukwa/archive/");
  static final int CHUNKCOUNT = 1000;
  

  
  /**
   * Writes a single chunk to a file, checks that archiver delivers it
   * to an archive file with correct filename.
   */
  public void testArchiving() throws Exception {
    FileSystem fileSys;
    MiniMRCluster mr;
    JobConf jc ;
    
    System.out.println("starting archive test");
    Configuration conf = new Configuration();
    
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 5);
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 2);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 2);
    conf.set(ChukwaArchiveDataTypeOutputFormat.GROUP_BY_CLUSTER_OPTION_NAME, "true");
    
    System.setProperty("hadoop.log.dir", System.getProperty(
        "test.build.data", "/tmp"));

    MiniDFSCluster dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true,
        null);
    fileSys = dfs.getFileSystem();
    conf.set("fs.default.name", fileSys.getUri().toString());
    
    System.out.println("filesystem is " + fileSys.getUri());

    
    mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri()
        .toString(), 1);
    jc = mr.createJobConf(new JobConf(conf));
   

    fileSys.delete(new Path("/chukwa"), true);//nuke sink

    writeASinkFile(jc, fileSys, DATASINKFILE, CHUNKCOUNT);
    
    FileStatus fstat = fileSys.getFileStatus(DATASINKFILE);
    long dataLen = fstat.getLen();
    assertTrue(dataLen > CHUNKCOUNT * 50);
    
    String[] archiveArgs = {"DataType", fileSys.getUri().toString() + DATASINK.toString(),
        fileSys.getUri().toString() +OUTPUT_DIR.toString() };
    
    assertEquals("true", jc.get("archive.groupByClusterName"));
    assertEquals(1, jc.getInt("io.sort.mb", 5));
    
    int returnVal = ToolRunner.run(jc,  new ChukwaArchiveBuilder(), archiveArgs);
    assertEquals(0, returnVal);
    fstat = fileSys.getFileStatus(DEST_FILE);
    assertEquals(dataLen, fstat.getLen());    
    
    Thread.sleep(1000);
    
    SinkArchiver a = new SinkArchiver();
    fileSys.delete(new Path("/chukwa"), true);

    writeASinkFile(jc, fileSys, DATASINKFILE, CHUNKCOUNT);
    writeASinkFile(jc, fileSys, DATASINK_NOTDONE, 50);
    writeASinkFile(jc, fileSys, DEST_FILE, 10);
    
    long doneLen = fileSys.getFileStatus(DATASINKFILE).getLen();
    long notDoneLen = fileSys.getFileStatus(DATASINK_NOTDONE).getLen();
    long archFileLen = fileSys.getFileStatus(DEST_FILE).getLen();

    //we now have three files: one closed datasink, one "unfinished" datasink,
    //and one archived.  After merge, should have two datasink files,
    //plus the "unfinished" datasink
    
    a.exec(fileSys, jc);

    browseDir(fileSys, new Path("/"), 0);    //OUTPUT_DIR, 0);
    
      //make sure we don't scramble anything
    assertEquals(notDoneLen, fileSys.getFileStatus(DATASINK_NOTDONE).getLen());
    assertEquals(archFileLen, fileSys.getFileStatus(DEST_FILE).getLen());
    //and make sure promotion worked right

    assertEquals(doneLen, fileSys.getFileStatus(MERGED_DATASINK).getLen());
    mr.shutdown();
    dfs.shutdown();
    
  }
  
}
