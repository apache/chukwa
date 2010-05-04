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

import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.datacollection.writer.SeqFileWriter;
import org.apache.hadoop.chukwa.datacollection.writer.localfs.LocalWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

/**
 * This class is used by LocalToRemoteHDFSMover to
 * convert .chukwa files to .done before moving them. 
 * By creating a new sequence file and copying all valid chunks to it,
 * it makes sure that no corrupt sequence files get into HDFS.
 */
public class CopySequenceFile {
  static Logger log = Logger.getLogger(CopySequenceFile.class);
  private static SequenceFile.Writer seqFileWriter = null;
  private static SequenceFile.Reader seqFileReader = null; 
  private static FSDataOutputStream newOutputStr = null;
  
  public static void createValidSequenceFile(Configuration conf,
		                                     String originalFileDir, 
		                                     String originalFileName,
		                                     FileSystem localFs) {
    try {
      if (!originalFileDir.endsWith("/")) {
    	      originalFileDir += "/";
      }	
	  String originalCompleteDir= originalFileDir + originalFileName;
	  Path originalPath= new Path(originalCompleteDir);
	  int extensionIndex= originalFileName.indexOf(".chukwa",0);
      
	  String recoverFileName=originalFileName.substring(0, extensionIndex)+".recover";
	  String recoverDir= originalFileDir + recoverFileName;
	  Path recoverPath= new Path(recoverDir);
	  String recoverDoneFileName=originalFileName.substring(0, extensionIndex)+".recoverDone";
 	  String recoverDoneDir= originalFileDir + recoverDoneFileName;
 	  Path recoverDonePath= new Path(recoverDoneDir);
	  String doneFileName=originalFileName.substring(0, extensionIndex)+".done";
	  String doneDir= originalFileDir + doneFileName;
	  Path donePath= new Path(doneDir);
	  
	  ChukwaArchiveKey key = new ChukwaArchiveKey();
      ChunkImpl evt = ChunkImpl.getBlankChunk();

	  newOutputStr = localFs.create(recoverPath);
      seqFileWriter = SequenceFile.createWriter(conf, newOutputStr,
                                                ChukwaArchiveKey.class, ChunkImpl.class,
                                                SequenceFile.CompressionType.NONE, null);
      seqFileReader = new SequenceFile.Reader(localFs, originalPath, conf);
        
      System.out.println("key class name is " + seqFileReader.getKeyClassName());
      System.out.println("value class name is " + seqFileReader.getValueClassName());
      try { 
        while (seqFileReader.next(key, evt)) {
          seqFileWriter.append(key, evt);
        }
       } catch (ChecksumException e) { //The exception occurs when we read a bad chunk while copying
           log.info("Encountered Bad Chunk while copying .chukwa file, continuing",e);	 
       }
       seqFileReader.close();
	   seqFileWriter.close();
	   newOutputStr.close();
       try {
	     localFs.rename(recoverPath, recoverDonePath); //Rename the destination file from .recover to .recoverDone 
   	     localFs.delete(originalPath,false); //Delete Original .chukwa file
	     localFs.rename(recoverDonePath, donePath); //rename .recoverDone to .done
       } catch (Exception e) {
           log.warn("Error occured while renaming .recoverDone to .recover or deleting .chukwa",e);		 
    	   e.printStackTrace();
       }

	} catch(Exception e) {
	    log.warn("Error during .chukwa file recovery",e);	 
	    e.printStackTrace();
	}	
  }
}
