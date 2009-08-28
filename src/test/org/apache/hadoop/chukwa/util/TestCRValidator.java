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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import junit.framework.TestCase;
import static org.apache.hadoop.chukwa.util.ConstRateValidator.ByteRange;
import static org.apache.hadoop.chukwa.util.ConstRateValidator.ValidatorSM;
import static org.apache.hadoop.chukwa.util.TempFileUtil.writeASinkFile;

public class TestCRValidator extends TestCase{
  
  public void testCRchunks() {
    ConstRateAdaptor adaptor = new ConstRateAdaptor();
    adaptor.parseArgs("500  200 ");
    adaptor.test_init("testdata");
    Chunk c = adaptor.nextChunk(100);
    assertTrue(ConstRateAdaptor.checkChunk(c));
    c = adaptor.nextChunk(102);
    assertTrue(ConstRateAdaptor.checkChunk(c));
  }
  
  public void testBasicSM() throws Exception {
    ValidatorSM sm = new ValidatorSM();
    byte[] dat = "test".getBytes();    
    ChunkImpl c = new ChunkImpl("Data", "aname", dat.length, dat, null);
    ByteRange b = new ByteRange(c);
    assertEquals(4, b.len);
    assertEquals(0, b.start);
    String t = sm.advanceSM(b);
    assertNull(t);
    if(t != null)
      System.out.println(t);

    dat = "ing".getBytes();
    c = new ChunkImpl("Data", "aname", dat.length+4, dat, null);
    b = new ByteRange(c);
    assertEquals(4, b.start);
    t = sm.advanceSM(b);
    assertNull(t);
    if(t != null)
      System.out.println(t);
    
   b = new ByteRange(new ChunkImpl("Data", "aname", 12, "more".getBytes(), null));
   t= sm.advanceSM(b);
   System.out.println(t);
  }
  
  public void testSlurping() throws Exception {
    int NUM_CHUNKS = 10;
    Configuration conf = new Configuration();
    FileSystem localfs = FileSystem.getLocal(conf);
    String baseDir = System.getProperty("test.build.data", "/tmp");
    Path tmpFile = new Path(baseDir+"/tmpSeqFile.seq");
    writeASinkFile(conf, localfs, tmpFile, NUM_CHUNKS);
     
    ValidatorSM sm = new ValidatorSM();
    
    try {
      SequenceFile.Reader reader = new SequenceFile.Reader(localfs, tmpFile, conf);

      ChukwaArchiveKey key = new ChukwaArchiveKey();
      ChunkImpl chunk = ChunkImpl.getBlankChunk();

      while (reader.next(key, chunk)) {
          String s = sm.advanceSM(new ByteRange(chunk));
          assertNull(s);
      }
      reader.close();
      assertEquals(NUM_CHUNKS, sm.chunks);      
      localfs.delete(tmpFile);
    } catch(IOException e) {
      e.printStackTrace();
    }
    
  }

}
