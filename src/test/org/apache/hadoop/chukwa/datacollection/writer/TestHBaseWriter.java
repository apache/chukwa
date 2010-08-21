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
package org.apache.hadoop.chukwa.datacollection.writer;


import java.util.ArrayList;

import junit.framework.Assert;
import junit.framework.TestCase;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.ChunkImpl;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.HBaseWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class TestHBaseWriter extends TestCase{
  static Logger log = Logger.getLogger(TestHBaseWriter.class);
  private HBaseTestingUtility util;
  private HBaseWriter hbw;
  private Configuration conf;
  private byte[] columnFamily = Bytes.toBytes("TestColumnFamily");
  private byte[] qualifier = Bytes.toBytes("Key");
  private byte[] expectedValue = Bytes.toBytes("Value");

  private byte[] table = Bytes.toBytes("Test");
  private byte[] test = Bytes.toBytes("1234567890 Key Value");
  private ChukwaConfiguration cc;
  long timestamp = 1234567890;
  
  public TestHBaseWriter() {
    cc = new ChukwaConfiguration();

    conf = HBaseConfiguration.create();
    conf.set("hbase.hregion.memstore.flush.size", String.valueOf(128*1024));
    try {
      util = new HBaseTestingUtility(conf);
      util.startMiniZKCluster();
      util.getConfiguration().setBoolean("dfs.support.append", true);
      util.startMiniCluster(2);
      HTableDescriptor desc = new HTableDescriptor();
      HColumnDescriptor family = new HColumnDescriptor(columnFamily);
      desc.setName(table);
      desc.addFamily(family);
      util.getHBaseAdmin().createTable(desc);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
  
  public void setup() {
    
  }
  
  public void tearDown() {
    
  }
  
  public void testWriters() {
    ArrayList<Chunk> chunks = new ArrayList<Chunk>();
    chunks.add(new ChunkImpl("TextParser", "name", timestamp, test, null));      
    try {      
      cc.set("hbase.demux.package", "org.apache.chukwa.datacollection.writer.test.demux");
      cc.set("TextParser","org.apache.hadoop.chukwa.datacollection.writer.test.demux.TextParser");
      conf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
      hbw = new HBaseWriter(cc, conf);
      hbw.init(cc);
      if(hbw.add(chunks)!=ChukwaWriter.COMMIT_OK) {
        Assert.fail("Commit status is not OK.");
      }
      HTable testTable = new HTable(table);
      ResultScanner scanner = testTable.getScanner(columnFamily, qualifier);
      for(Result res : scanner) {
        Assert.assertEquals(new String(expectedValue), new String(res.getValue(columnFamily, qualifier)));
      }
      // Cleanup and return
      scanner.close();
      // Compare data in Hbase with generated chunks
      util.shutdownMiniCluster();
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail(e.getMessage());
    }
  }
}
