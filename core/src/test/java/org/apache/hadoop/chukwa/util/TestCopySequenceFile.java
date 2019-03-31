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
import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.io.IOException;

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.util.CopySequenceFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class TestCopySequenceFile extends TestCase {
  File doneFile = null;
  File tempDir = null;
  String tempFileName = null;
	
  public void testCopySequenceFile() throws IOException {
    //Create a .chukwa sequence file 
	  
    tempDir = new File(System.getProperty("test.build.data", "/tmp"));
    File tempFile = File.createTempFile("testcopy", ".chukwa", tempDir); 
    tempFile.deleteOnExit(); // Will delete this file if test fails and file is not renamed to .done
    tempFileName=tempFile.getName();
    Configuration conf = new Configuration();
    Path path = new Path(tempFile.getAbsolutePath());
    List<ChunkImpl> chunks = new ArrayList<ChunkImpl>();
    byte[] dat = "test".getBytes();
    
    ChunkImpl c = new ChunkImpl("Data", "aname", dat.length, dat, null);
    chunks.add(c);
    
    dat = "ing".getBytes();
    c = new ChunkImpl("Data", "aname", dat.length+4, dat, null);
    chunks.add(c);
    
    //Utilize the writeSeqFile method to create a valid .chukwa sequence file
    
    writeSeqFile(conf, FileSystem.getLocal(conf), path, chunks);
    
    //Call CopySequenceFile to convert .chukwa to .done
    
    CopySequenceFile.createValidSequenceFile(conf, tempDir.getAbsolutePath(), tempFile.getName(), FileSystem.getLocal(conf));
	
    //Assert that the chukwa file has been deleted
    
	assertFalse("File " + tempFile.getAbsolutePath() + " has not been deleted", tempFile.exists()) ; 
	
	String doneFilePath= tempDir.getAbsolutePath()+"/"+tempFileName.replace(".chukwa", ".done");
	doneFile= new File(doneFilePath);
	
	//Assert that the done file has been created
	
    assertTrue("File " + doneFilePath + " has not been created", doneFile.exists()); 
		
  }
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
  protected void tearDown() {
	if (doneFile != null && doneFile.exists()){
		  doneFile.delete();
		} else { //Cleanup any files that may have been created during a failed copy attempt 
			File recoverFile = new File(tempDir.getAbsolutePath()+"/"+tempFileName.replace(".chukwa", ".recover"));
			if (recoverFile.exists()){
			  recoverFile.delete();
			} else {
			    File recoverDoneFile = new File(tempDir.getAbsolutePath()+"/"+tempFileName.replace(".chukwa", ".recoverDone"));
			    if (recoverDoneFile.exists()){
			      recoverDoneFile.delete();
			    }
			  }
		 }
  }
}	
