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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.hadoop.chukwa.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public class ConstRateValidator extends Configured implements Tool{
  
  public static class ByteRange implements WritableComparable<ByteRange> {
    
    String stream;
    String split ="";
    public long start;
    public long len;
    
    public ByteRange() {
      start=len=0;
    }
    
    public ByteRange(ChunkImpl val) {
      
      len = val.getLength();
      start = val.getSeqID() - len;     
      this.stream = val.getSource()+":"+val.getStreamName() ;
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
      stream = in.readUTF();
      split = in.readUTF();
      start = in.readLong();
      len = in.readLong();
    }
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeUTF(stream);
      out.writeUTF(split);
      out.writeLong(start);
      out.writeLong(len);
    }

    public static ByteRange read(DataInput in) throws IOException {
      ByteRange b = new ByteRange();
      b.readFields(in);
      return b;
    }
    
    @Override
    public int compareTo(ByteRange o) {
      int c = stream.compareTo(o.stream);
      if(c != 0)
        return c;
      
      if(start > o.start)
        return 1;
      else if (start < o.start)
        return -1;
      else {
        if(len > o.len)
          return 1;
        else if(len < o.len)
          return -1;
        else
          return split.compareTo(o.split);
      }
    }
    
    public boolean equals(Object o) {
      if(o instanceof ByteRange) {
        ByteRange rhs = (ByteRange) o;
        return stream.equals(rhs.stream) &&
         split.equals(rhs.split)&& rhs.start == start && rhs.len == len;
      } else
        return false;
    }
    
    public int hashCode() {
      return (int) (
          stream.hashCode() ^ (len>>32) ^ (len & 0xFFFFFFFF) ^ (start >> 32)
          ^ (start & 0xFFFFFFFF));
    } 
  }
  
  
  
  ///////  State machine; expects chunks in order ////////
  public static class ValidatorSM {
    public long ok=0, missingBytes=0,dupBytes=0;
    long consecDupchunks=0;
    long nextExpectedStart = 0;
    public long chunks;
    public long dupChunks;
    public Set<String> filesContaining = new LinkedHashSet<String>();

    public String closeSM() {
      if(consecDupchunks > 0)
        return consecDupchunks + " consecutive duplicate chunks ending at " + consecDupchunks;
      else
        return null;
    }
    
    public String advanceSM(ByteRange b) {
      if(!b.split.equals(""))
        filesContaining.add(b.split);
      
      chunks++;
      
      if(b.start == nextExpectedStart) {
        String msg = null;
        if(consecDupchunks > 0)
          msg = consecDupchunks + " consecutive duplicative chunks ending at " + b.start;
        consecDupchunks = 0;
        nextExpectedStart += b.len;
        ok += b.len;
        return msg;
      } else{
//        Text msg = new Text(b.stream + " " + consecOKchunks + 
//            "consecutive OK chunks ending at " + nextExpectedStart);
        String msg;
        if(b.start < nextExpectedStart) {    //duplicate bytes
          consecDupchunks ++;
          dupChunks++;
          long duplicatedBytes;
          if(b.start + b.len <= nextExpectedStart) {
            duplicatedBytes = b.len;
            msg =" dupchunk of length " + b.len + " at " + b.start;
          } else {
            duplicatedBytes = b.start + b.len - nextExpectedStart;
            ok += b.len - duplicatedBytes;
            msg = "  overlap of " + duplicatedBytes+ " starting at " + b.start +
            " (total chunk len ="+b.len+")";
          }
          dupBytes += duplicatedBytes;
          nextExpectedStart = Math.max(b.start + b.len, nextExpectedStart);
        } else {  //b.start > nextExpectedStart  ==>  missing bytes
          consecDupchunks = 0;
          long missing = (b.start - nextExpectedStart);
          msg = "==Missing "+ missing+ " bytes starting from " + nextExpectedStart;
          nextExpectedStart = b.start + b.len;
          
          if(b.start < 0 || b.len < 0)
            System.out.println("either len or start was negative; something is seriously wrong");
          
          missingBytes += missing;
        }
        return msg;
      } //end not-OK  
    } //end advance
  } //end class
  
  
  ///////  Map Class /////////
  public static class MapClass extends Mapper <ChukwaArchiveKey, ChunkImpl, ByteRange, NullWritable> {
    
    @Override
    protected void map(ChukwaArchiveKey key, ChunkImpl val, 
        Mapper<ChukwaArchiveKey, ChunkImpl,ByteRange, NullWritable>.Context context)
        throws IOException, InterruptedException 
    {
      boolean valid = ConstRateAdaptor.checkChunk(val);
      String fname = "unknown";
      
      ByteRange ret = new ByteRange(val);
      
      InputSplit inSplit = context.getInputSplit();
      if(inSplit instanceof FileSplit) {
        FileSplit fs = (FileSplit) inSplit;
        fname = fs.getPath().getName();
      }
      ret.split = fname;
      
      if(!valid) {
        context.getCounter("app", "badchunks").increment(1);
      }
      context.write(ret, NullWritable.get());
    }
  }
    
  public static class ReduceClass extends Reducer<ByteRange, NullWritable, Text,Text> {
    
    ValidatorSM sm;
    String curStream = "";
    
    public ReduceClass() {
      sm = new ValidatorSM();
    }
    
//    @Override
//    protected void setup(Reducer<ByteRange, NullWritable, Text,Text>.Context context) {       }
    
    @Override
    protected void reduce(ByteRange b, Iterable<NullWritable> vals, 
        Reducer<ByteRange, NullWritable, Text,Text>.Context context) {
      try {

      if(!curStream.equals(b.stream)) {
        if(!curStream.equals("")) {
          printEndOfStream(context);
        }
        
        System.out.println("rolling over to new stream " + b.stream);
        curStream = b.stream;
        sm = new ValidatorSM();
      }
      
      String msg = sm.advanceSM(b);
      if(msg != null)
        context.write(new Text(b.stream), new Text(msg));

    } catch(InterruptedException e) {
    } catch(IOException e) {
      e.printStackTrace();
    }
  }
    
    @Override
    protected void cleanup(Reducer<ByteRange, NullWritable, Text,Text>.Context context)
    throws IOException, InterruptedException{
      printEndOfStream(context);
    }
    
    public void printEndOfStream(Reducer<ByteRange, NullWritable, Text,Text>.Context context) 
    throws IOException, InterruptedException {
      Text cs = new Text(curStream);

      String t = sm.closeSM();
      if(t != null)
        context.write(cs, new Text(t));
      if(!sm.filesContaining.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        sb.append("Data contained in");
        for(String s: sm.filesContaining) 
          sb.append(" ").append(s);
        context.write(cs, new Text(sb.toString()));
      }
      context.write(cs, new Text("total of " + sm.chunks + " chunks ("
         + sm.dupChunks + " dups). " +" High byte =" + (sm.nextExpectedStart-1)));
      
      context.getCounter("app", "missing bytes").increment(sm.missingBytes);
      context.getCounter("app", "duplicate bytes").increment(sm.dupBytes);
      context.getCounter("app", "OK Bytes").increment(sm.ok);
    }
  } //end reduce class


  public static void main(String[] args) throws Exception {
 //   System.out.println("specify -D textOutput=true for text output");
    int res = ToolRunner.run(new Configuration(),
        new ConstRateValidator(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] real_args) throws Exception {
    GenericOptionsParser gop = new GenericOptionsParser(getConf(), real_args);
    Configuration conf = gop.getConfiguration();
    String[] args = gop.getRemainingArgs();

    Job validate = new Job(conf);
    
    validate.setJobName("Chukwa Test pattern validator");
    validate.setJarByClass(this.getClass());
    
    validate.setInputFormatClass(SequenceFileInputFormat.class);
    
    validate.setMapperClass(MapClass.class);
    validate.setMapOutputKeyClass(ByteRange.class);
    validate.setMapOutputValueClass(NullWritable.class);

    validate.setReducerClass(ReduceClass.class);
    validate.setOutputFormatClass(TextOutputFormat.class);

    
    FileInputFormat.setInputPaths(validate, new Path(args[0]));
    FileOutputFormat.setOutputPath(validate, new Path(args[1]));

    validate.submit();
    return 0;
  }

}
