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

import junit.framework.Assert;

import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.chukwa.util.GenerateTestFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.ExecType;

public class TestLocalChukwaStorage extends PigTest {

  protected ExecType getExecType() {
    return ExecType.LOCAL;
  }

  public void testLocal() {

    File tempDir = new File(System.getProperty("test.build.data", "/tmp"));
    if (!tempDir.exists()) {
      tempDir.mkdirs();
    }
    String directory = tempDir.getAbsolutePath() + "/TestLocalChukwaStorage_"
        + System.currentTimeMillis() + "/";
System.out.println(directory);
    FileSystem fs = null;
    Configuration conf = null;
    
    try {
      conf = new Configuration();
      fs = FileSystem.getLocal(conf);
      GenerateTestFile.fs =fs;
      GenerateTestFile.conf = conf;
      GenerateTestFile.createFile(directory);

      File buildDir = new File(System.getProperty("chukwa.root.build.dir", "../../build/"));
      String[] files = buildDir.list();
      for (String f : files) {
        if (f.startsWith("chukwa-core") && f.endsWith(".jar")) {
          log.info("Found" + buildDir.getAbsolutePath() + "/" + f);
          pigServer.registerJar(buildDir.getAbsolutePath() + "/" + f);
          break;
        }
      }

      pigServer.registerJar(System.getProperty("chukwa-pig.build.dir", "../../build/") + "/chukwa-pig.jar");
      
      pigServer.registerQuery("A = load '"
              + directory
              + "/chukwaTestFile.evt' using  org.apache.hadoop.chukwa.pig.ChukwaLoader() as (ts: long,fields);");
      pigServer.registerQuery("B = FOREACH A GENERATE ts,'myCluster',fields,fields#'csource','myRecord',fields#'csource','myApplication', fields#'A';");
      pigServer.registerQuery("define seqWriter org.apache.hadoop.chukwa.pig.ChukwaStorer('c_timestamp', 'c_cluster' ,'fields','c_pk','c_recordtype','c_source','c_application','myFieldA');");
      pigServer.registerQuery("STORE B into '" + directory
          + "/chukwa-pig.evt' using seqWriter;");

      try {
        String res = dumpArachive(fs,conf,directory+ "chukwa-pig.evt/part-m-00000");
        String expected = "===== KEY   =====DataType: myRecordKey: 1242000000/M0/1242205800===== Value =====Timestamp : 1242205800[A] :7[B] :3[C] :9[capp] :myApplication[csource] :M0[ctags] : cluster=\"myCluster\" [myFieldA] :7===== KEY   =====DataType: myRecordKey: 1242000000/M0/1242205800===== Value =====Timestamp : 1242205800[D] :1[capp] :myApplication[csource] :M0[ctags] : cluster=\"myCluster\" ===== KEY   =====DataType: myRecordKey: 1242000000/M1/1242205800===== Value =====Timestamp : 1242205800[A] :17[capp] :myApplication[csource] :M1[ctags] : cluster=\"myCluster\" [myFieldA] :17===== KEY   =====DataType: myRecordKey: 1242000000/M1/1242205800===== Value =====Timestamp : 1242205800[B] :37[C] :51[capp] :myApplication[csource] :M1[ctags] : cluster=\"myCluster\" ===== KEY   =====DataType: myRecordKey: 1242000000/M0/1242205860===== Value =====Timestamp : 1242205860[A] :8[C] :3[D] :12[capp] :myApplication[csource] :M0[ctags] : cluster=\"myCluster\" [myFieldA] :8===== KEY   =====DataType: myRecordKey: 1242000000/M0/1242205860===== Value =====Timestamp : 1242205860[A] :8[B] :6[capp] :myApplication[csource] :M0[ctags] : cluster=\"myCluster\" [myFieldA] :8===== KEY   =====DataType: myRecordKey: 1242000000/M1/1242205860===== Value =====Timestamp : 1242205860[A] :13.2[B] :23[C] :8.5[D] :6[capp] :myApplication[csource] :M1[ctags] : cluster=\"myCluster\" [myFieldA] :13.2===== KEY   =====DataType: myRecordKey: 1242000000/M1/1242205860===== Value =====Timestamp : 1242205860[A] :13.2[B] :23[C] :8.5[D] :6[capp] :myApplication[csource] :M1[ctags] : cluster=\"myCluster\" [myFieldA] :13.2===== KEY   =====DataType: myRecordKey: 1242000000/M0/1242205920===== Value =====Timestamp : 1242205920[A] :8[B] :6[C] :8[D] :6[E] :48.5[capp] :myApplication[csource] :M0[ctags] : cluster=\"myCluster\" [myFieldA] :8===== KEY   =====DataType: myRecordKey: 1242000000/M1/1242205920===== Value =====Timestamp : 1242205920[A] :8.3[B] :5.2[C] :37.7[D] :61.9[E] :40.3[capp] :myApplication[csource] :M1[ctags] : cluster=\"myCluster\" [myFieldA] :8.3===== KEY   =====DataType: myRecordKey: 1242000000/M1/1242205980===== Value =====Timestamp : 1242205980[A] :18.3[B] :1.2[C] :7.7[capp] :myApplication[csource] :M1[ctags] : cluster=\"myCluster\" [myFieldA] :18.3===== KEY   =====DataType: myRecordKey: 1242000000/M2/1242205980===== Value =====Timestamp : 1242205980[A] :8.9[B] :8.3[C] :7.2[D] :6.1[capp] :myApplication[csource] :M2[ctags] : cluster=\"myCluster\" [myFieldA] :8.9===== KEY   =====DataType: myRecordKey: 1242000000/M3/1242205920===== Value =====Timestamp : 1242205920[A] :12.5[B] :26.82[C] :89.51[capp] :myApplication[csource] :M3[ctags] : cluster=\"myCluster\" [myFieldA] :12.5===== KEY   =====DataType: myRecordKey: 1242000000/M4/1242205920===== Value =====Timestamp : 1242205920[A] :13.91[B] :21.02[C] :18.05[capp] :myApplication[csource] :M4[ctags] : cluster=\"myCluster\" [myFieldA] :13.91";
        log.info("res[" + res + "]");
        Assert.assertTrue("expected result differ from current result",res.equals(expected));

        log.info(res);
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.fail();
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

  protected String dumpArachive(FileSystem fs, Configuration conf, String file)
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
