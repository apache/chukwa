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

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;



public class GenerateTestFile {

/* Pig Test:
  A = load './chukwaTestFile.evt' using  org.apache.hadoop.chukwa.pig.ChukwaLoader() as (ts: long,fields);
  Dump A;
 
  (1242205800L,[A#7,B#3,csource#M0,C#9])
  (1242205800L,[D#1,csource#M0])
  (1242205800L,[A#17,csource#M1])
  (1242205800L,[B#37,C#51,csource#M1])
  (1242205860L,[D#12,A#8,csource#M0,C#3])
  (1242205860L,[A#8,B#6,csource#M0])
  (1242205860L,[D#6,A#13.2,B#23,C#8.5,csource#M1])
  (1242205860L,[D#6,A#13.2,B#23,C#8.5,csource#M1])
  (1242205920L,[D#6,E#48.5,A#8,B#6,C#8,csource#M0])
  (1242205920L,[D#61.9,E#40.3,A#8.3,B#5.2,C#37.7,csource#M1])
  (1242205980L,[A#18.3,B#1.2,csource#M1,C#7.7])
  (1242205980L,[D#6.1,A#8.9,B#8.3,C#7.2,csource#M2])
  (1242205920L,[A#12.5,B#26.82,csource#M3,C#89.51])
  (1242205920L,[A#13.91,B#21.02,csource#M4,C#18.05])
  
 B = group A by (ts,fields#'csource');
 Dump B;
 
 ((1242205800L,M0),{(1242205800L,[A#7,B#3,csource#M0,C#9]),(1242205800L,[D#1,csource#M0])})
 ((1242205800L,M1),{(1242205800L,[A#17,csource#M1]),(1242205800L,[B#37,C#51,csource#M1])})
 ((1242205860L,M0),{(1242205860L,[D#12,A#8,csource#M0,C#3]),(1242205860L,[A#8,B#6,csource#M0])})
 ((1242205860L,M1),{(1242205860L,[D#6,A#13.2,B#23,C#8.5,csource#M1]),(1242205860L,[D#6,A#13.2,B#23,C#8.5,csource#M1])})
 ((1242205920L,M0),{(1242205920L,[D#6,E#48.5,A#8,B#6,C#8,csource#M0])})
 ((1242205920L,M1),{(1242205920L,[D#61.9,E#40.3,A#8.3,B#5.2,C#37.7,csource#M1])})
 ((1242205920L,M3),{(1242205920L,[A#12.5,B#26.82,csource#M3,C#89.51])})
 ((1242205920L,M4),{(1242205920L,[A#13.91,B#21.02,csource#M4,C#18.05])})
 ((1242205980L,M1),{(1242205980L,[A#18.3,B#1.2,csource#M1,C#7.7])})
 ((1242205980L,M2),{(1242205980L,[D#6.1,A#8.9,B#8.3,C#7.2,csource#M2])})

 C = FOREACH B GENERATE group.$0,group.$1,org.apache.hadoop.chukwa.RecordMerger(A.fields);
 Dump C;
 (1242205800L,M0,[D#1,A#7,B#3,csource#M0,C#9])
 (1242205800L,M1,[A#17,B#37,C#51,csource#M1])
 (1242205860L,M0,[D#12,A#8,B#6,csource#M0,C#3])
 (1242205860L,M1,[D#6,A#13.2,B#23,csource#M1,C#8.5])
 (1242205920L,M0,[D#6,E#48.5,A#8,B#6,csource#M0,C#8])
 (1242205920L,M1,[D#61.9,E#40.3,A#8.3,B#5.2,csource#M1,C#37.7])
 (1242205920L,M3,[A#12.5,B#26.82,C#89.51,csource#M3])
 (1242205920L,M4,[A#13.91,B#21.02,C#18.05,csource#M4])
 (1242205980L,M1,[A#18.3,B#1.2,C#7.7,csource#M1])
 (1242205980L,M2,[D#6.1,A#8.9,B#8.3,csource#M2,C#7.2])

 
*/

  public static Configuration conf = null;
  public static FileSystem fs = null;
  
  public static void main(String[] args) throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    createFile(null);
  }
  
    public static void createFile(String path) throws Exception {
      
      Path outputFile = null; 
    if (path != null) {
      outputFile = new Path(path + "/chukwaTestFile.evt");
    } else {
      outputFile = new Path("chukwaTestFile.evt");
    }

    outputFile = outputFile.makeQualified(fs);
    if (fs.exists(outputFile)) {
      System.out.println("File already there, exit -1," + outputFile );
      System.exit(-1);
    }
    System.out.println("outputFile:" + outputFile);
    
    SequenceFile.Writer seqFileWriter = SequenceFile.createWriter(fs, conf,outputFile, ChukwaRecordKey.class,ChukwaRecord.class,CompressionType.NONE);
    ChukwaRecordKey key = new ChukwaRecordKey();
    key.setReduceType("TestSeqFile");
    
   
    
    String chukwaKey = "";
    String machine = "";
    String TimePartion = "1242205200"; //Wed, 13 May 2009 09:00:00 GMT
    
    {    
      
      machine = "M0";
      long time = 1242205800; // Wed, 13 May 2009 09:10:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "7");
      record.add("B", "3");
      record.add("C", "9");
  
      seqFileWriter.append(key, record);
    }

    {    
      machine = "M0";
      long time = 1242205800; // Wed, 13 May 2009 09:10:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("D", "1");
  
      seqFileWriter.append(key, record);
    }

    {    
      machine = "M1";
      long time = 1242205800; // Wed, 13 May 2009 09:10:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "17");
  
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M1";
      long time = 1242205800; // Wed, 13 May 2009 09:10:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("B", "37");
      record.add("C", "51");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M0";
      long time = 1242205860; // Wed, 13 May 2009 09:10:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "8");
      record.add("C", "3");
      record.add("D", "12");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M0";
      long time = 1242205860; // Wed, 13 May 2009 09:11:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "8");
      record.add("B", "6");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M1";
      long time = 1242205860; // Wed, 13 May 2009 09:11:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "13.2");
      record.add("B", "23");
      record.add("C", "8.5");
      record.add("D", "6");
      
      // create duplicate
      seqFileWriter.append(key, record);
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M0";
      long time = 1242205920; // Wed, 13 May 2009 09:12:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "8");
      record.add("B", "6");
      record.add("C", "8");
      record.add("D", "6");
      record.add("E", "48.5");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M1";
      long time = 1242205920; // Wed, 13 May 2009 09:12:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "8.3");
      record.add("B", "5.2");
      record.add("C", "37.7");
      record.add("D", "61.9");
      record.add("E", "40.3");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M1";
      long time = 1242205980; // Wed, 13 May 2009 09:13:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "18.3");
      record.add("B", "1.2");
      record.add("C", "7.7");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M2";
      long time = 1242205980; // Wed, 13 May 2009 09:13:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "8.9");
      record.add("B", "8.3");
      record.add("C", "7.2");
      record.add("D", "6.1");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M3";
      // late arrival T0
      long time = 1242205920; // Wed, 13 May 2009 09:12:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "12.5");
      record.add("B", "26.82");
      record.add("C", "89.51");
      seqFileWriter.append(key, record);
    }
    
    {    
      machine = "M4";
      // late arrival T0
      long time = 1242205920; // Wed, 13 May 2009 09:12:00 GMT
      chukwaKey = TimePartion +"/" + machine +"/" + time;
      key.setKey(chukwaKey);
      
      ChukwaRecord record = new ChukwaRecord();
      record.setTime(time);
      record.add("csource", machine);
      record.add("A", "13.91");
      record.add("B", "21.02");
      record.add("C", "18.05");
      seqFileWriter.append(key, record);
    }
    
    seqFileWriter.close();
  }
}
