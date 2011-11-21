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


import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

/**
 * Tool for exploring the contents of the Chukwa data archive, or a collection
 * of Chukwa sequence files.
 * 
 * Limitation: DumpArchive infers the filesystem to dump from based on the first
 * path argument, and will behave strangely if you try to dump files
 * from different filesystems in the same invocation.
 *
 */
public class DumpArchive {

  static boolean summarize = false;
  
  static HashMap<String, Integer> counts  = new LinkedHashMap<String, Integer>();
  /**
   * @param args
   * @throws URISyntaxException
   * @throws IOException
   */
  public static void main(String[] args) throws IOException, URISyntaxException {

    int firstArg = 0;
    if(args.length == 0) {
      System.out.println("Usage: DumpArchive [--summarize] <sequence files>");
    }
    if(args[0].equals("--summarize")) {
      firstArg = 1;
      summarize= true;
    } 
    ChukwaConfiguration conf = new ChukwaConfiguration();
    FileSystem fs;
    if(args[firstArg].contains("://")) {
      fs = FileSystem.get(new URI(args[firstArg]), conf);
    } else {
      String fsName = conf.get("writer.hdfs.filesystem");
      if(fsName != null)
        fs = FileSystem.get(conf);
      else
        fs = FileSystem.getLocal(conf);
    }
    ArrayList<Path> filesToSearch = new ArrayList<Path>();
    for(int i=firstArg; i < args.length; ++i){
      Path[] globbedPaths = FileUtil.stat2Paths(fs.globStatus(new Path(args[i])));
      for(Path p: globbedPaths)
        filesToSearch.add(p);
    }
    int tot = filesToSearch.size();
    int i=1;

    System.err.println("total of " + tot + " files to search");
    for(Path p: filesToSearch) {
      System.err.println("scanning " + p.toUri() + "("+ (i++) +"/"+tot+")");
      dumpFile(p, conf, fs);
    }

    if(summarize) {
      for(Map.Entry<String, Integer> count: counts.entrySet()) {
        System.out.println(count.getKey()+ ")   ===> " + count.getValue());
      }
    }
  }

  private static void dumpFile(Path p, Configuration conf,
      FileSystem fs) throws IOException {
    SequenceFile.Reader r = new SequenceFile.Reader(fs, p, conf);

    ChukwaArchiveKey key = new ChukwaArchiveKey();
    ChunkImpl chunk = ChunkImpl.getBlankChunk();
    try {
      while (r.next(key, chunk)) {
        
        String entryKey = chunk.getSource() +":"+chunk.getDataType() +":" +
        chunk.getStreamName();
        
        Integer oldC = counts.get(entryKey);
        if(oldC != null)
          counts.put(entryKey, oldC + 1);
        else
          counts.put(entryKey, new Integer(1));
        
        if(!summarize) {
          System.out.println("\nTimePartition: " + key.getTimePartition());
          System.out.println("DataType: " + key.getDataType());
          System.out.println("StreamName: " + key.getStreamName());
          System.out.println("SeqId: " + key.getSeqId());
          System.out.println("\t\t =============== ");
  
          System.out.println("Cluster : " + chunk.getTags());
          System.out.println("DataType : " + chunk.getDataType());
          System.out.println("Source : " + chunk.getSource());
          System.out.println("Application : " + chunk.getStreamName());
          System.out.println("SeqID : " + chunk.getSeqID());
          System.out.println("Data : " + new String(chunk.getData()));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
