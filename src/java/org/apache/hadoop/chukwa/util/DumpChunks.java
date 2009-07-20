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
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.RecordUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.conf.Configuration;

public class DumpChunks {

  static class SearchRule {
    Pattern p;
    String targ;
    
    SearchRule(Pattern p, String t) {
      this.p = p;
      this.targ = t;
    }
    
    boolean matches(ChunkImpl chunk) {
      if(targ.equals("datatype")) {
        return p.matcher(chunk.getDataType()).matches();
      } else if(targ.equals("name")) {
        return p.matcher(chunk.getStreamName()).matches();
      } else if(targ.equals("host")) {
        return p.matcher(chunk.getSource()).matches();
      } else if(targ.equals("cluster")) {
        String cluster = RecordUtil.getClusterName(chunk);
        return p.matcher(cluster).matches();
      }
      else { 
        assert false: "unknown target: " +targ;
        return false;
      }
    }
    
  }
  
  static final String[] SEARCH_TARGS = {"datatype", "name", "host", "cluster"};

    static final String SEPARATOR="&";
  /**
   * Tries to find chunks matching a given pattern.
   * Takes as input a set of &-delimited patterns, followed
   * by a list of file names.
   * 
   * E.g:  Dump datatype=Iostat&source=/my/log/.* *.done
   */
  public static void main(String[] args) throws IOException, URISyntaxException {
    
    if(args.length < 2) {
      System.out.println("usage: Dump pattern1,pattern2,pattern3... file1 file2 file3...");
      System.exit(-1);
    }
    System.err.println("Patterns:" + args[0]);
    for(int i=1; i < args.length; ++i)
        System.err.println("FileGlob: " + args[i]);

    ChukwaConfiguration conf = new ChukwaConfiguration();
    FileSystem fs;
    if(args[1].contains("://")) {
      fs = FileSystem.get(new URI(args[1]), conf);
    } else {
      String fsName = conf.get("writer.hdfs.filesystem");
      if(fsName == null)
        fs = FileSystem.getLocal(conf);
      else
        fs = FileSystem.get(conf);
    }
    System.err.println("filesystem is " + fs.getUri());

    dump(args, conf, fs, System.out);
  }

  static void dump(String[] args, Configuration conf,
      FileSystem fs, PrintStream out) throws IOException {
    List<SearchRule> patterns = buildPatterns(args[0]);
    ArrayList<Path> filesToSearch = new ArrayList<Path>();

    Map<String, SortedMap<Long, ChunkImpl> > matchCatalog = new HashMap<String, SortedMap<Long, ChunkImpl> >();
    
    for(int i=1; i < args.length; ++i){
      Path[] globbedPaths = FileUtil.stat2Paths(fs.globStatus(new Path(args[i])));
      for(Path p: globbedPaths)
        filesToSearch.add(p);
    }
    
    System.err.println("expands to " + filesToSearch.size() + " actual files");

    try {
      for(Path p: filesToSearch) {
      
        SequenceFile.Reader r = new SequenceFile.Reader(fs, p, conf);
  
        ChukwaArchiveKey key = new ChukwaArchiveKey();
        ChunkImpl chunk = ChunkImpl.getBlankChunk();
        while (r.next(key, chunk)) {
          if(matchesPattern(patterns, chunk)) {
            updateMatchCatalog(matchCatalog, key.getStreamName(), chunk);
            chunk = ChunkImpl.getBlankChunk();
          }
        }
      }
      
      for(SortedMap<Long, ChunkImpl> stream: matchCatalog.values()) {
        printNoDups(stream, out);
        out.println("\n--------------------");
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
  
  private static void printNoDups(SortedMap<Long, ChunkImpl> stream, OutputStream out) throws IOException {
    long nextToPrint = 0;

   System.err.println("---- map starts at "+ stream.firstKey());
    for(Map.Entry<Long, ChunkImpl> e: stream.entrySet()) {
      if(e.getKey() >= nextToPrint) {
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
    
  }

  private static void updateMatchCatalog(
      Map<String, SortedMap<Long, ChunkImpl>> matchCatalog, String streamName,
      ChunkImpl chunk) {

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

  static List<SearchRule> buildPatterns(String listOfPatterns) throws
  PatternSyntaxException{
    List<SearchRule> compiledPatterns = new ArrayList<SearchRule>();
    //FIXME: could escape these
    String[] patterns = listOfPatterns.split(SEPARATOR);
    for(String p: patterns) {
      int equalsPos = p.indexOf('=');
      
      if(equalsPos < 0 || equalsPos > (p.length() -2)) {
        throw new PatternSyntaxException(
            "pattern must be of form targ=pattern", p, -1);
      }
      
      String targ = p.substring(0, equalsPos);
      if(!ArrayUtils.contains(SEARCH_TARGS, targ)) {
        throw new PatternSyntaxException(
            "pattern doesn't start with recognized search target", p, -1);
      }
      
      Pattern pat = Pattern.compile(p.substring(equalsPos+1));
      compiledPatterns.add(new SearchRule(pat, targ));
    }
    
    return compiledPatterns;
  }

  static boolean matchesPattern(List<SearchRule> matchers, ChunkImpl chunk) {
    for(SearchRule r: matchers) {
      if(!r.matches(chunk))
        return false;
    }
    return true;
  }

}
