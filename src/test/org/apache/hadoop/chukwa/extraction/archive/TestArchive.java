/*
 * Copyright (C) The Apache Software Foundation. All rights reserved.
 *
 * This software is published under the terms of the Apache Software
 * License version 1.1, a copy of which has been included with this
 * distribution in the LICENSE.txt file.  */

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

public class TestArchive extends TestCase {

  java.util.Random r = new java.util.Random();
  
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
  
  public ChunkImpl getARandomChunk() {
    int ms = r.nextInt(1000);
    String line = "2008-05-29 10:42:22," + ms
        + " INFO org.apache.hadoop.dfs.DataNode: Some text goes here"
        + r.nextInt() + "\n";

    ChunkImpl c = new ChunkImpl("HadoopLogProcessor", "test",
        line.length() - 1L, line.getBytes(), null);
    c.addTag("cluster=\"foocluster\"");
    return c;
  }
  
  public void writeASinkFile(Configuration conf, FileSystem fileSys, Path dest,
      int chunks) throws IOException {
    FSDataOutputStream out = fileSys.create(dest);

    Calendar calendar = Calendar.getInstance();
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, out,
        ChukwaArchiveKey.class, ChunkImpl.class,
        SequenceFile.CompressionType.NONE, null);
    for (int i = 0; i < chunks; ++i) {
      ChunkImpl chunk = getARandomChunk();
      ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();

      calendar.set(Calendar.YEAR, 2008);
      calendar.set(Calendar.MONTH, Calendar.MAY);
      calendar.set(Calendar.DAY_OF_MONTH, 29);
      calendar.set(Calendar.HOUR, 10);
      calendar.set(Calendar.MINUTE, 0);
      calendar.set(Calendar.SECOND, 0);
      calendar.set(Calendar.MILLISECOND, 0);
      archiveKey.setTimePartition(calendar.getTimeInMillis());
      archiveKey.setDataType(chunk.getDataType());
      archiveKey.setStreamName(chunk.getStreamName());
      archiveKey.setSeqId(chunk.getSeqID());
      seqFileWriter.append(archiveKey, chunk);
    }
    seqFileWriter.close();
    out.close();
  }

  static final int NUM_HADOOP_SLAVES = 1;
  static final Path DATASINK = new Path("/chukwa/logs/*");
  static final Path DATASINKFILE = new Path("/chukwa/logs/foobar.done");
  static final Path OUTPUT_DIR = new Path("/chukwa/archives/");
  
  /**
   * Writes a single chunk to a file, checks that archiver delivers it
   * to an archive file with correct filename.
   */
  public void testArchiving() throws Exception {
    
    System.out.println("starting archive test");
    Configuration conf = new Configuration();
    System.setProperty("hadoop.log.dir", System.getProperty(
        "test.build.data", "/tmp"));
    MiniDFSCluster dfs = new MiniDFSCluster(conf, NUM_HADOOP_SLAVES, true,
        null);
    FileSystem fileSys = dfs.getFileSystem();
    fileSys.delete(OUTPUT_DIR, true);//nuke output dir

    writeASinkFile(conf, fileSys, DATASINKFILE, 1000);
    
    FileStatus fstat = fileSys.getFileStatus(DATASINKFILE);
    assertTrue(fstat.getLen() > 10);
    
    System.out.println("filesystem is " + fileSys.getUri());
    conf.set("fs.default.name", fileSys.getUri().toString());
    conf.setInt("io.sort.mb", 1);
    conf.setInt("io.sort.factor", 5);
    conf.setInt("mapred.tasktracker.map.tasks.maximum", 2);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 2);
    conf.set("archive.addClusterName", "true");
    
    MiniMRCluster mr = new MiniMRCluster(NUM_HADOOP_SLAVES, fileSys.getUri()
        .toString(), 1);
    String[] archiveArgs = {"DataType", fileSys.getUri().toString() + DATASINK.toString(),
        fileSys.getUri().toString() +OUTPUT_DIR.toString() };
    
    JobConf jc = mr.createJobConf(new JobConf(conf));
    assertEquals("true", jc.get("archive.groupByClusterName"));
    assertEquals(1, jc.getInt("io.sort.mb", 5));
    
    int returnVal = ToolRunner.run(jc,  new ChukwaArchiveBuilder(), archiveArgs);
    assertEquals(0, returnVal);
    fstat = fileSys.getFileStatus(new Path("/chukwa/archives/foocluster/HadoopLogProcessor_2008_05_29.arc"));
    assertTrue(fstat.getLen() > 10);    
    
    Thread.sleep(1000);
    browseDir(fileSys, new Path("/"), 0);    //OUTPUT_DIR, 0);
    System.out.println("done!");
    
    
    
  }
  
}
