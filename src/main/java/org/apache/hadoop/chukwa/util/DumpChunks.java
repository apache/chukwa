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
import java.util.regex.*;
import java.util.*;
import java.io.*;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

public class DumpChunks {

  
  /**
   * Tries to find chunks matching a given pattern.
   * Takes as input a set of &-delimited patterns, followed
   * by a list of file names.
   * 
   * E.g:  Dump datatype=Iostat&source=/my/log/.* *.done
   */
  public static void main(String[] args) throws IOException, URISyntaxException {
    
    if(args.length < 2) {
      System.out.println("usage: Dump [-s] pattern1,pattern2,pattern3... file1 file2 file3...");
      System.exit(-1);
    }
    
    ChukwaConfiguration conf = new ChukwaConfiguration();

    dump(args, conf, System.out);
  }
  
  static FileSystem getFS(Configuration conf, String uri) throws IOException, URISyntaxException {
    FileSystem fs;
    if(uri.contains("://")) {
      fs = FileSystem.get(new URI(uri), conf);
    } else {
      String fsName = conf.get("writer.hdfs.filesystem");
      if(fsName == null)
        fs = FileSystem.getLocal(conf);
      else
        fs = FileSystem.get(conf);
    }
    System.err.println("filesystem is " + fs.getUri());
    return fs;
  }

  static void dump(String[] args, Configuration conf, PrintStream out) throws IOException, URISyntaxException {
    
    int filterArg = 0;
    boolean summarize = false;
    boolean nosort = false;
    if(args[0].equals("-s")) {
      filterArg++;
      summarize = true;
    } else if(args[0].equals("--nosort")) {
      filterArg++;
      nosort = true;
    }
    
    Filter patterns;
    if(args[filterArg].toLowerCase().equals("all"))
      patterns = Filter.ALL;
    else
      patterns = new Filter(args[filterArg]);

    System.err.println("Patterns:" + patterns);
    ArrayList<Path> filesToSearch = new ArrayList<Path>();

    FileSystem fs = getFS(conf, args[filterArg + 1]);
    for(int i=filterArg + 1; i < args.length; ++i){
      Path[] globbedPaths = FileUtil.stat2Paths(fs.globStatus(new Path(args[i])));
      if(globbedPaths != null)
        for(Path p: globbedPaths)
          filesToSearch.add(p);
    }
    
    System.err.println("expands to " + filesToSearch.size() + " actual files");

    DumpChunks dc;
    if(summarize)
      dc = new DumpAndSummarize();
    else if(nosort)
      dc = new DumpNoSort(out);
    else
      dc= new DumpChunks();
    
    try {
      for(Path p: filesToSearch) {
      
        SequenceFile.Reader r = new SequenceFile.Reader(fs, p, conf);
  
        ChukwaArchiveKey key = new ChukwaArchiveKey();
        ChunkImpl chunk = ChunkImpl.getBlankChunk();
        while (r.next(key, chunk)) {
          if(patterns.matches(chunk)) {
            dc.updateMatchCatalog(key.getStreamName(), chunk);
            chunk = ChunkImpl.getBlankChunk();
          }
        }
      }
      
      dc.displayResults(out);
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public DumpChunks() {
    matchCatalog = new HashMap<String, SortedMap<Long, ChunkImpl> >();
  }

  Map<String, SortedMap<Long, ChunkImpl>> matchCatalog;
  
  protected void displayResults(PrintStream out) throws IOException{
    for(Map.Entry<String,SortedMap<Long, ChunkImpl>> streamE: matchCatalog.entrySet()) {
      String header = streamE.getKey();
      SortedMap<Long, ChunkImpl> stream = streamE.getValue();
      long nextToPrint = 0;
      if(stream.firstKey() > 0)
        System.err.println("---- map starts at "+ stream.firstKey());
      for(Map.Entry<Long, ChunkImpl> e: stream.entrySet()) {
        if(e.getKey() >= nextToPrint) {
          if(e.getKey() > nextToPrint)
            System.err.println("---- printing bytes starting at " + e.getKey());
          
          out.write(e.getValue().getData());
          nextToPrint = e.getValue().getSeqID();
        } else if(e.getValue().getSeqID() < nextToPrint) {
          continue; //data already printed
        } else {
          //tricky case: chunk overlaps with already-printed data, but not completely
          ChunkImpl c = e.getValue();
          long chunkStartPos = e.getKey();
          int numToPrint = (int) (c.getSeqID() - nextToPrint);
          int printStartOffset = (int) ( nextToPrint -  chunkStartPos);
          out.write(c.getData(), printStartOffset, numToPrint);
          nextToPrint = c.getSeqID();
        }
      }
      out.println("\n--------"+header + "--------");
    }
  }
 
  protected void updateMatchCatalog(String streamName,  ChunkImpl chunk) throws IOException {

    SortedMap<Long, ChunkImpl> chunksInStream = matchCatalog.get(streamName);
    if(chunksInStream == null ) {
      chunksInStream = new TreeMap<Long, ChunkImpl>();
      matchCatalog.put(streamName, chunksInStream);
    }
    
    long startPos = chunk.getSeqID() - chunk.getLength();
    
    ChunkImpl prevMatch = chunksInStream.get(startPos);
    if(prevMatch == null)
      chunksInStream.put(startPos, chunk);
    else { //pick longest
      if(chunk.getLength() > prevMatch.getLength())
        chunksInStream.put (startPos, chunk);
    }
  }

  static class DumpAndSummarize extends DumpChunks {
    Map<String, Integer> matchCounts = new LinkedHashMap<String, Integer>();
    Map<String, Long> byteCounts = new LinkedHashMap<String, Long>();
    

    protected void displayResults(PrintStream out) throws IOException{
      for(Map.Entry<String, Integer> s: matchCounts.entrySet()) {
        out.print(s.getKey());
        out.print(" ");
        out.print(s.getValue());
        out.print(" chunks ");
        out.print(byteCounts.get(s.getKey()));
        out.println(" bytes");
      }
        
    }
    
    protected void updateMatchCatalog(String streamName,  ChunkImpl chunk) {
      Integer i = matchCounts.get(streamName);
      if(i != null) {
        matchCounts.put(streamName, i+1);
        Long b = byteCounts.get(streamName);
        byteCounts.put(streamName, b + chunk.getLength());
      } else {
        matchCounts.put(streamName, new Integer(1));
        byteCounts.put(streamName, new Long(chunk.getLength()));
      }
    }
    
  }
  
  static class DumpNoSort extends DumpChunks {
    
    PrintStream out; 
    public DumpNoSort(PrintStream out) {
      this.out = out;
    }
    //Do some display
    protected void updateMatchCatalog(String streamName,  ChunkImpl chunk) throws IOException {
      out.write(chunk.getData());
    }
    
    protected void displayResults(PrintStream out) throws IOException{
      ; //did this in updateMatchCatalog
    }
    
  }

}
