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

package org.apache.hadoop.chukwa.util;


import java.io.*;
import java.util.Calendar;
import java.util.Random;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class TempFileUtil {
  public static File makeBinary(int length) throws IOException {
    File tmpOutput = new File(System.getProperty("test.build.data", "/tmp"),
        "chukwaTest");
    FileOutputStream fos = new FileOutputStream(tmpOutput);
    Random r = new Random();
    byte[] randomData = new byte[length];
    r.nextBytes(randomData);
    randomData[length - 1] = '\n';// need data to end with \n since default
                                  // tailer uses that
    fos.write(randomData);
    fos.flush();
    fos.close();
    return tmpOutput;
  }
  

  static class RandSeqFileWriter {
    java.util.Random r = new java.util.Random();
    long lastSeqID = 0;
    
    public ChunkImpl getARandomChunk() {
      int ms = r.nextInt(1000);
      String line = "2008-05-29 10:42:22," + ms
          + " INFO org.apache.hadoop.dfs.DataNode: Some text goes here"
          + r.nextInt() + "\n";
       
      ChunkImpl c = new ChunkImpl("HadoopLogProcessor", "test",
      line.length()  + lastSeqID, line.getBytes(), null);
      lastSeqID += line.length();
      c.addTag("cluster=\"foocluster\"");
      return c;
    }
  }
  
  public static void writeASinkFile(Configuration conf, FileSystem fileSys, Path dest,
       int chunks) throws IOException {
     FSDataOutputStream out = fileSys.create(dest);

     Calendar calendar = Calendar.getInstance();
     SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, out,
         ChukwaArchiveKey.class, ChunkImpl.class,
         SequenceFile.CompressionType.NONE, null);
     RandSeqFileWriter rw = new RandSeqFileWriter();
     for (int i = 0; i < chunks; ++i) {
       ChunkImpl chunk = rw.getARandomChunk();
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
   

   public static File makeTestFile(String name, int size,File baseDir) throws IOException {
     File tmpOutput = new File(baseDir, name);
     FileOutputStream fos = new FileOutputStream(tmpOutput);

     PrintWriter pw = new PrintWriter(fos);
     for (int i = 0; i < size; ++i) {
       pw.print(i + " ");
       pw.println("abcdefghijklmnopqrstuvwxyz");
     }
     pw.flush();
     pw.close();
     return tmpOutput;
   }
   

   public static File makeTestFile(String name, int size) throws IOException {
     return makeTestFile(name, size, new File(System.getProperty("test.build.data", "/tmp")));

   }
   
   public static File makeTestFile(File baseDir) throws IOException {
     return makeTestFile("atemp",10, baseDir);
   }
   

   public static File makeTestFile() throws IOException {
     return makeTestFile("atemp",80, new File(System.getProperty("test.build.data", "/tmp")));
   }
  
}
