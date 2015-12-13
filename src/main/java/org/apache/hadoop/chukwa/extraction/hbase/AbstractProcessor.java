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

package org.apache.hadoop.chukwa.extraction.hbase;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Reporter;
import org.apache.hadoop.chukwa.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

public abstract class AbstractProcessor {
  static Logger LOG = Logger.getLogger(AbstractProcessor.class);

  protected int entryCount = 0;
  protected String primaryKeyHelper;
  protected String sourceHelper;

  protected byte[] key = null;
  byte[] CF = "t".getBytes(Charset.forName("UTF-8"));

  boolean chunkInErrorSaved = false;
  ArrayList<Put> output = null;
  ArrayList<Put> meta = null;
  Reporter reporter = null;
  long time = System.currentTimeMillis();
  Chunk chunk = null;
  MessageDigest md5 = null;

  public AbstractProcessor() throws NoSuchAlgorithmException {
    md5 = MessageDigest.getInstance("md5");
  }

  protected abstract void parse(byte[] recordEntry) throws Throwable;

  /**
   * Generic metric function to add a metric to HBase with full primary key and
   * source computed.
   * 
   * @param time
   * @param metric
   * @param source
   * @param value
   * @param output
   */
  public void addRecord(long time, String metric, String source, byte[] value,
      ArrayList<Put> output) {
    String primaryKey = new StringBuilder(primaryKeyHelper).append(".")
        .append(metric).toString();
    byte[] key = HBaseUtil.buildKey(time, primaryKey, source);
    Put put = new Put(key);
    byte[] timeInBytes = ByteBuffer.allocate(8).putLong(time).array();
    put.addColumn(CF, timeInBytes, time, value);
    output.add(put);
    reporter.putMetric(chunk.getDataType(), primaryKey);
    reporter.putSource(chunk.getDataType(), source);
  }

  public void addRecord(String primaryKey, String value) {
    addRecord(primaryKey, value.getBytes(Charset.forName("UTF-8")));
  }

  /**
   * Generic function to add a metric to HBase metric table, this function
   * assumes "time" and "source" have been defined and will construct primaryKey
   * only, without recompute time and source md5.
   * 
   * @param metric
   * @param value
   */
  public void addRecord(String metric, byte[] value) {
    String primaryKey = new StringBuilder(primaryKeyHelper).append(".")
        .append(metric).toString();
    byte[] key = HBaseUtil.buildKey(time, primaryKey, sourceHelper);
    Put put = new Put(key);
    byte[] timeInBytes = ByteBuffer.allocate(8).putLong(time).array();
    put.addColumn(CF, timeInBytes, time, value);
    output.add(put);
    reporter.putMetric(chunk.getDataType(), primaryKey);
  }

  /**
   * Process a chunk to store in HBase.
   * 
   * @param chunk
   * @param output
   * @param reporter
   * @throws Throwable
   */
  public void process(Chunk chunk, ArrayList<Put> output, Reporter reporter)
      throws Throwable {
    this.output = output;
    this.reporter = reporter;
    this.chunk = chunk;
    this.primaryKeyHelper = chunk.getDataType();
    this.sourceHelper = chunk.getSource();
    reporter.putSource(primaryKeyHelper, sourceHelper);
    parse(chunk.getData());
    addMeta();
  }

  protected void addMeta() {
    byte[] key = HBaseUtil.buildKey(time, chunk.getDataType(), sourceHelper);
    Put put = new Put(key);
    String family = "a";
    byte[] timeInBytes = ByteBuffer.allocate(8).putLong(time).array();
    put.addColumn(family.getBytes(Charset.forName("UTF-8")), timeInBytes, time, chunk.getTags().getBytes(Charset.forName("UTF-8")));
    output.add(put);
  }

}
