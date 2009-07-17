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
package edu.berkeley.chukwa_xtrace;

import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.AbstractProcessor;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.extraction.engine.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;

import edu.berkeley.xtrace.reporting.Report;
import edu.berkeley.xtrace.*;

/**
 * MapReduce job to process xtrace reports coming out of chukwa demux.
 * 
 * Map phase unwraps the chukwa records, reduce phase does trace reconstruction.
 * 
 * We use task ID as the reduce sort key.
 *
 */
public class XtrExtract extends Configured implements Tool {
  
  public static final String OUTLINK_FIELD = "__xtr_outlinks";
  
  /**
   * with more than 10,000 reports, switch to on-disk sort, 
   * instead of in-memory topological sort.
   */
  static final int MAX_IN_MEMORY_REPORTS = 10* 1000;
  
public static class MapClass extends Mapper <Object, Object, BytesWritable, Text> {
    
    public MapClass() {
      System.out.println("starting xtrace map");
    }
    
    @Override
    protected void map(Object k, Object v, 
        Mapper<Object, Object,BytesWritable, Text>.Context context)
        throws IOException, InterruptedException 
    {
      Text t;
      BytesWritable bw;
      
      if(k instanceof ChukwaArchiveKey && v instanceof ChunkImpl) {
        ChunkImpl value = (ChunkImpl) v;
        Report xtrReport = Report.createFromString(new String(value.getData()));
        bw = new BytesWritable(xtrReport.getMetadata().getTaskId().get());
        //FIXME: can probably optimize the above lines by doing a search in the raw bytes
        t= new Text(value.getData());
      } else if(k instanceof ChukwaRecordKey && v instanceof ChukwaRecord){
        ChukwaRecord value = (ChukwaRecord) v;
        Report xtrReport = Report.createFromString(value.getValue(Record.bodyField));
        bw = new BytesWritable(xtrReport.getMetadata().getTaskId().get());
        //FIXME: can probably optimize the above lines by doing a search in the raw bytes
        t= new Text(value.getValue(Record.bodyField));
      } else {
        System.out.println("unexpected key/value types: "+ k.getClass().getCanonicalName() 
            + " and " + v.getClass().getCanonicalName() );
        return;
      }
      context.write(bw, t);
    }
  }

  public static class Reduce extends Reducer<BytesWritable, Text,BytesWritable,ArrayWritable> {
    
    public Reduce() {}
    
    /**
     * 
     * Note that loading everything into hashtables means
     * we implicity suppress duplicate-but-identical reports.  
     * 
     */
    protected  void reduce(BytesWritable taskID, Iterable<Text> values, 
          Reducer<BytesWritable, Text,BytesWritable,ArrayWritable>.Context context) 
          throws IOException, InterruptedException
    {
      String taskIDString = new String(taskID.getBytes());
      //in both cases, key is OpId string
      HashMap<String, Report> reports = new LinkedHashMap<String, Report>();

      Counter reportCounter = context.getCounter("app", "reports");
      Counter edgeCounter = context.getCounter("app", "edges");
      Counter badEdgeCounter = context.getCounter("app", "reference to missing report");
      int edgeCount = 0;
      
      int numReports = 0;
      for(Text rep_text: values) {
        Report r = Report.createFromString(rep_text.toString());
        numReports++;
        
        if(numReports < MAX_IN_MEMORY_REPORTS) {
          reports.put(r.getMetadata().getOpIdString(), r);
        } else if(numReports == MAX_IN_MEMORY_REPORTS) {
          //bail out, prepare to do an external sort.
          return;
        } else
          ;
    //      do the external sort
      }

      HashMap<String, Integer> counts = new HashMap<String, Integer>();
      Queue<Report> zeroInlinkReports = new LinkedList<Report>();
      reportCounter.increment(reports.size());
      //FIXME: could usefully compare reports.size() with numReports;
      //that would measure duplicate reports
      
      //increment link counts for children
      for(Report r: reports.values()){ 
        String myOpID = r.getMetadata().getOpIdString();
        int parentCount = 0;
        for(String inLink: r.get("Edge")) {
          
            //sanitize data from old, nonconformant C++ implementation
          if(inLink.contains(","))
            inLink = inLink.substring(0, inLink.indexOf(','));
          
          Report parent = reports.get(inLink);
          if(parent != null) {
            parent.put(OUTLINK_FIELD, myOpID);
            parentCount++;
            edgeCount++;
          }
          else { //no match
            if(!inLink.equals("0000000000000000"))  {
              System.out.println("no sign of parent: " + inLink);
              badEdgeCounter.increment(1);
            }
            //else quietly suppress
          }
        }
          
        //if there weren't any parents, we can dequeue
        if(parentCount == 0)
          zeroInlinkReports.add(r);
        else
          counts.put(myOpID, parentCount);
      }
      
      System.out.println(taskIDString+": " + edgeCount + " total edges");
      edgeCounter.increment(edgeCount);
      //at this point, we have a map from metadata to report, and also
      //from report op ID to inlink count.
      //next step is to do a topological sort.

      
      Text[] finalOutput = new Text[reports.size()];
      System.out.println(taskIDString+": expecting to sort " + finalOutput.length + " reports");
      int i=0;
      while(!zeroInlinkReports.isEmpty()) {
        Report r = zeroInlinkReports.poll();
        if(r == null) {
          System.err.println("poll returned null but list not empty");
          break;
        }
        finalOutput[i++] = new Text(r.toString());
        List<String> outLinks =  r.get(OUTLINK_FIELD);
        if(outLinks != null) {
          for(String outLink: outLinks) {
            Integer oldCount = counts.get(outLink);
            if(oldCount == null) {
              oldCount = 0;  //FIXME: can this happen?
              System.out.println("warning: found an in-edge where none was expected");
            } if(oldCount == 1) {
              zeroInlinkReports.add(reports.get(outLink));
            }
            counts.put(outLink, oldCount -1);
          }
        }
      }
      if(i != finalOutput.length) {
        if(i > 0)
           System.out.println("error: I only sorted " + i + " items, but expected " 
            + finalOutput.length+", is your list cyclic?");
        else
          System.out.println("every event in graph has a predecessor; perhaps "
              + "the start event isn't in the input set?");
      }

      context.write(taskID, new ArrayWritable(Text.class, finalOutput));
      //Should sort values topologically and output list.  or?
      
    } //end reduce
    
  }//end reduce class


  @Override
  public int run(String[] arg) throws Exception {
    Job extractor = new Job(getConf());
    

    extractor.setMapperClass(MapClass.class);
    
    extractor.setReducerClass(Reduce.class);
    extractor.setJobName("x-trace reconstructor");
    extractor.setJarByClass(this.getClass());
    
    extractor.setMapOutputKeyClass(BytesWritable.class);
    extractor.setMapOutputValueClass(Text.class);
    
    extractor.setOutputKeyClass(BytesWritable.class);
    extractor.setOutputValueClass(ArrayWritable.class);
    
    extractor.setInputFormatClass(SequenceFileInputFormat.class);
    extractor.setOutputFormatClass(SequenceFileOutputFormat.class);
    FileInputFormat.setInputPaths(extractor, new Path(arg[0]));
    FileOutputFormat.setOutputPath(extractor, new Path(arg[1]));
    System.out.println("looks OK.  Submitting.");
    extractor.submit();
//    extractor.waitForCompletion(false);
    return 0;

  }

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(),
        new XtrExtract(), args);
    System.exit(res);
  }

}
