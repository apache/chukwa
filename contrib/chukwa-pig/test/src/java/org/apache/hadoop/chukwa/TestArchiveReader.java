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

package org.apache.hadoop.chukwa;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import junit.framework.Assert;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.GenerateTestFile;
import org.apache.hadoop.chukwa.util.TempFileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.*;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.*;
/**
 * Note that this test will NOT work if run from eclipse.
 * 
 * Pig needs a jarfile, and therefore the test makes fairly strong
 * assumptions about its environment.  It'll work correctly 
 * if you do ant test. 
 *
 */
public class TestArchiveReader extends PigTest {

  protected ExecType getExecType() {
    return ExecType.LOCAL;
  }

  public void testLocal() {

    File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    String directory = tempDir.getAbsolutePath() + "/TestArchiveChukwaStorage_"
        + System.currentTimeMillis() + "/";
    System.out.println(directory);
    FileSystem fs = null;
    Configuration conf = null;
    
    try {
      conf = new Configuration();
      fs = FileSystem.getLocal(conf);
      Path seqFile = new Path(directory, "test.seq");
      TempFileUtil.writeASinkFile(conf, fs, seqFile, 10);

     File buildDir = new File(System.getProperty("chukwa.root.build.dir", "../../build/"));
//     File buildDir = new File(System.getProperty("chukwa.root.build.dir", 
 //            "/Users/asrabkin/workspace/chukwa_trunk/build"));

      String[] files = buildDir.list();
      for (String f : files) {
        if (f.startsWith("chukwa-core") && f.endsWith(".jar")) {
          log.info("Found" + buildDir.getAbsolutePath() + "/" + f);
          pigServer.registerJar(buildDir.getAbsolutePath() + "/" + f);
          break;
        }
      }
      String pigJarDir = System.getProperty("chukwa-pig.build.dir", "../../build/");
 //     pigJarDir = "/Users/asrabkin/workspace/chukwa_trunk/contrib/chukwa-pig";
      pigServer.registerJar(pigJarDir + "/chukwa-pig.jar");
      
      pigServer.registerQuery("A = load '" + seqFile.toString()
            + "' using  org.apache.hadoop.chukwa.ChukwaArchive()"
    //       +" as (ts: long,fields);");
            + ";");
     // pigServer.registerQuery("B = FOREACH A GENERATE ts,'myCluster',fields,fields#'csource','myRecord',fields#'csource','myApplication', fields#'A';");
    //  pigServer.registerQuery("define seqWriter org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp', 'c_cluster' ,'fields','c_pk','c_recordtype','c_source','c_application','myFieldA');");
    //  pigServer.registerQuery("STORE B into '" + directory
   //       + "/chukwa-pig.evt' using seqWriter;");

      Schema schema_A = pigServer.dumpSchema("A");
      assertTrue(schema_A.equals(ChukwaArchive.chukwaArchiveSchema));
 //     pigServer.explain("A", System.out);
 
//      pigServer.registerQuery("B = DUMP A");
      pigServer.registerQuery("B = FOREACH A GENERATE seqNo;");
     
      Iterator<Tuple> chunks = pigServer.openIterator("B");
      if(!chunks.hasNext())
        System.out.println("WARN: I expected to get some seqNos");
      while(chunks.hasNext()) {
        System.out.println(chunks.next());
      }
      
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    } finally {
      if (fs != null) {
        try {
          fs.delete(new Path(directory), true);
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      pigServer.shutdown();
    }
  }

  protected String dumpArchive(FileSystem fs, Configuration conf, String file)
      throws Throwable {
    SequenceFile.Reader reader = null;
    log.info("File: [" +  file + "]" + fs.exists(new Path(file)));
    try {
      reader = new SequenceFile.Reader(fs, new Path(file), conf);

      ChukwaRecordKey key = new ChukwaRecordKey();
      ChukwaRecord record = new ChukwaRecord();

      StringBuilder sb = new StringBuilder();
      while (reader.next(key, record)) {
       
        sb.append("===== KEY   =====");

        sb.append("DataType: " + key.getReduceType());
        sb.append("Key: " + key.getKey());
        sb.append("===== Value =====");

        String[] fields = record.getFields();
        Arrays.sort(fields );
        sb.append("Timestamp : " + record.getTime());
        for (String field : fields) {
          sb.append("[" + field + "] :" + record.getValue(field));
        }
      }
      
      return sb.toString();
    } catch (Throwable e) {
      e.printStackTrace();
      Assert.fail("Exception while reading SeqFile" + e.getMessage());
      throw e;
    }
    finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
