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
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecord;
import org.apache.hadoop.chukwa.extraction.engine.ChukwaRecordKey;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;

public class DumpRecord {

  /**
   * @param args
   * @throws URISyntaxException
   * @throws IOException
   */
  public static void main(String[] args) throws IOException, URISyntaxException {
    System.out.println("Input file:" + args[0]);

    ChukwaConfiguration conf = new ChukwaConfiguration();
    String fsName = conf.get("writer.hdfs.filesystem");
    FileSystem fs = FileSystem.get(new URI(fsName), conf);

    SequenceFile.Reader r = new SequenceFile.Reader(fs, new Path(args[0]), conf);

    ChukwaRecordKey key = new ChukwaRecordKey();
    ChukwaRecord record = new ChukwaRecord();
    try {
      while (r.next(key, record)) {
        System.out.println("\t ===== KEY   ===== ");

        System.out.println("DataType: " + key.getReduceType());
        System.out.println("\nKey: " + key.getKey());
        System.out.println("\t ===== Value =====");

        String[] fields = record.getFields();
        System.out.println("Timestamp : " + record.getTime());
        for (String field : fields) {
          System.out.println("[" + field + "] :" + record.getValue(field));
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

}
