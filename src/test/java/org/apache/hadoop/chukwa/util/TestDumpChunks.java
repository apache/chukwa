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

import junit.framework.TestCase;
import java.util.*;
import java.io.*;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.DataFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class TestDumpChunks extends TestCase {
  
  public static void writeSeqFile(Configuration conf, FileSystem fileSys, Path dest,
      List<ChunkImpl> chunks) throws IOException {
    FSDataOutputStream out = fileSys.create(dest);

    Calendar calendar = Calendar.getInstance();
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(conf, out,
        ChukwaArchiveKey.class, ChunkImpl.class,
        SequenceFile.CompressionType.NONE, null);
    
    for (ChunkImpl chunk: chunks) {
      ChukwaArchiveKey archiveKey = new ChukwaArchiveKey();
      
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

  public void testFilePatternMatching() throws IOException, java.net.URISyntaxException {
    
    File tempDir = new File(System.getProperty("test.build.data", "/tmp"));

    File tmpFile = File.createTempFile("dumpchunkTest", ".seq", tempDir);
    tmpFile.deleteOnExit();
    
    Configuration conf = new Configuration();
    Path path = new Path(tmpFile.getAbsolutePath());
    List<ChunkImpl> chunks = new ArrayList<ChunkImpl>();
    byte[] dat = "test".getBytes();
    
    ChunkImpl c = new ChunkImpl("Data", "aname", dat.length, dat, null);
    chunks.add(c);
    
    dat = "ing".getBytes();
    c = new ChunkImpl("Data", "aname", dat.length+4, dat, null);
    chunks.add(c);
    
    writeSeqFile(conf, FileSystem.getLocal(conf), path, chunks);
    
    String[] args = new String[] {"datatype=Data",path.toString()};
    ByteArrayOutputStream capture = new ByteArrayOutputStream();
    DumpChunks.dump(args, conf,new PrintStream(capture));
    
    assertTrue(new String(capture.toByteArray()).startsWith("testing\n---"));
    //now test for matches.
    
  }

}
