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

  private static class SearchRule {
    Pattern p;
    String targ;
    
    SearchRule(Pattern p, String t) {
      this.p = p;
      this.targ = t;
    }
    
    boolean matches(Chunk chunk) {
      if(targ.equals("datatype")) {
        return p.matcher(chunk.getDataType()).matches();
      } else if(targ.equals("name")) {
        return p.matcher(chunk.getStreamName()).matches();
      } else if(targ.equals("host")) {
        return p.matcher(chunk.getSource()).matches();
      } else if(targ.equals("cluster")) {
        String cluster = RecordUtil.getClusterName(chunk);
        return p.matcher(cluster).matches();
      } else if(targ.equals("content")) {
        String content = new String(chunk.getData());
        Matcher m = p.matcher(content);
        return m.matches();
      }
      else { 
        assert false: "unknown target: " +targ;
        return false;
      }
    }
    
    public String toString() {
      return targ + "=" +p.toString();
    }
    
  }
  
  public static class Filter {
    List<SearchRule> compiledPatterns;
    
    public Filter(String listOfPatterns) throws  PatternSyntaxException{
      compiledPatterns = new ArrayList<SearchRule>();
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
        
        Pattern pat = Pattern.compile(p.substring(equalsPos+1), Pattern.DOTALL);
        compiledPatterns.add(new SearchRule(pat, targ));
      }
    }

    public boolean matches(Chunk chunk) {
      for(SearchRule r: compiledPatterns) {
        if(!r.matches(chunk))
          return false;
      }
      return true;
    }
    
    public int size() {
      return compiledPatterns.size();
    }
    
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(compiledPatterns.get(0));
      for(int i=1; i < compiledPatterns.size(); ++i) {
        sb.append(" & ");
        sb.append(compiledPatterns.get(i));
      }
      return sb.toString();
    }
  }//end class
  
  static final String[] SEARCH_TARGS = {"datatype", "name", "host", "cluster", "content"};

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
    
    for(int i=1; i < args.length; ++i)
        System.err.println("FileGlob: " + args[i]);

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
    if(args[0].equals("-s")) {
      filterArg++;
      summarize = true;
    }
    
    Filter patterns = new Filter(args[filterArg]);

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
    for(SortedMap<Long, ChunkImpl> stream: matchCatalog.values()) {
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
      out.println("\n--------------------");
    }
  }
 
  protected void updateMatchCatalog(String streamName,  ChunkImpl chunk) {

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
    

    protected void displayResults(PrintStream out) throws IOException{
      for(Map.Entry<String, Integer> s: matchCounts.entrySet()) {
        out.print(s.getKey());
        out.print(" ");
        out.println(s.getValue());
      }
        
    }
    
    protected void updateMatchCatalog(String streamName,  ChunkImpl chunk) {
      Integer i = matchCounts.get(streamName);
      if(i != null)
        matchCounts.put(streamName, i+1);
      else
        matchCounts.put(streamName, new Integer(1));
    }
    
  }

}
