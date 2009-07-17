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
package edu.berkeley.chukwa_xtrace;

import java.io.IOException;
import java.util.Calendar;
import junit.framework.TestCase;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.extraction.archive.ChukwaArchiveBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;

public class TestXtrExtract extends TestCase {
  
  public void writeASinkFile(Configuration conf, FileSystem fileSys, Path dest,
      int chunks) throws IOException {
    FSDataOutputStream out = fileSys.create(dest);

    Calendar calendar = Calendar.getInstance();
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, out,
        ChukwaArchiveKey.class, ChunkImpl.class,
        SequenceFile.CompressionType.NONE, null);

      //FIXME: do write here

    seqFileWriter.close();
    out.close();
  }
  
  static final int NUM_HADOOP_SLAVES = 1;
  static final Path OUTPUT_DIR = new Path("/test/out/");
  static final Path INPUT_DIR = new Path("/test/in/");
  
 public void testArchiving() throws Exception {
    
    System.out.println("starting archive test");
    Configuration conf = new Configuration();
    System.setProperty("hadoop.log.dir", System.getProperty(
        "test.build.data", "/tmp"));
    MiniDFSCluster dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true,
        null);
    FileSystem fileSys = dfs.getFileSystem();
    fileSys.delete(OUTPUT_DIR, true);//nuke output dir

    writeASinkFile(conf, fileSys, INPUT_DIR, 1000);
    
    FileStatus fstat = fileSys.getFileStatus(INPUT_DIR);
    assertTrue(fstat.getLen() > 10);
    
    System.out.println("filesystem is " + fileSys.getUri());
    conf.set("fs.default.name", fileSys.getUri().toString());
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 5);
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 2);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 2);
    
    MiniMRCluster mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri()
        .toString(), 1);
    String[] archiveArgs = {INPUT_DIR.toString(),
        fileSys.getUri().toString() +OUTPUT_DIR.toString() };
    
    
    JobConf jc = mr.createJobConf(new JobConf(conf));
    assertEquals("true", jc.get("archive.groupByClusterName"));
    assertEquals(1, jc.getInt("io.sort.mb", 5));
    
    int returnVal = ToolRunner.run(jc,  new XtrExtract(), archiveArgs);
    assertEquals(0, returnVal);
    fstat = fileSys.getFileStatus(new Path("/chukwa/archives/foocluster/HadoopLogProcessor_2008_05_29.arc"));
    assertTrue(fstat.getLen() > 10);    
    
    Thread.sleep(1000);

    System.out.println("done!");
 }  
}
