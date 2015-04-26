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

package org.apache.hadoop.chukwa.datacollection.writer.hbase;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.extraction.hbase.AbstractProcessor;
import org.apache.hadoop.chukwa.extraction.hbase.ProcessorFactory;
import org.apache.hadoop.chukwa.extraction.hbase.UnknownRecordTypeException;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;

public class HBaseWriter extends PipelineableWriter {
  static Logger log = Logger.getLogger(HBaseWriter.class);
  private static final String CHUKWA_TABLE = "chukwa";
  private static final String CHUKWA_META_TABLE = "chukwa_meta";
  boolean reportStats;
  volatile long dataSize = 0;
  final Timer statTimer;
  private ArrayList<Put> output;
  private Reporter reporter;
  private ChukwaConfiguration conf;
  String defaultProcessor;
  private static Connection connection;
  
  private class StatReportingTask extends TimerTask {
    private long lastTs = System.currentTimeMillis();
    private long lastDataSize = 0;

    public void run() {
      long time = System.currentTimeMillis();
      long interval = time - lastTs;
      lastTs = time;

      long ds = dataSize;
      long dataRate = 1000 * (ds - lastDataSize) / interval; // bytes/sec
      // refers only to data field, not including http or chukwa headers
      lastDataSize = ds;

      log.info("stat=HBaseWriter|dataRate="
          + dataRate);
    }
  };

  public HBaseWriter() throws IOException {
    this(true);
  }

  public HBaseWriter(boolean reportStats) throws IOException {
    /* HBase Version >= 0.89.x */
    this(reportStats, new ChukwaConfiguration(), HBaseConfiguration.create());
  }

  public HBaseWriter(ChukwaConfiguration conf, Configuration hconf) throws IOException {
    this(true, conf, hconf);
  }

  private HBaseWriter(boolean reportStats, ChukwaConfiguration conf, Configuration hconf) throws IOException {
    this.reportStats = reportStats;
    this.conf = conf;
    this.statTimer = new Timer();
    this.defaultProcessor = conf.get(
      "chukwa.demux.mapper.default.processor",
      "org.apache.hadoop.chukwa.extraction.demux.processor.mapper.DefaultProcessor");
    log.info("hbase.zookeeper.quorum: " + hconf.get(HConstants.ZOOKEEPER_QUORUM) + ":" + hconf.get(HConstants.ZOOKEEPER_CLIENT_PORT));
    if (reportStats) {
      statTimer.schedule(new StatReportingTask(), 1000, 10 * 1000);
    }
    output = new ArrayList<Put>();
    try {
      reporter = new Reporter();
    } catch (NoSuchAlgorithmException e) {
      throw new IOException("Can not register hashing algorithm.");
    }
    if (connection == null) {
      connection = ConnectionFactory.createConnection(hconf);
    }
  }

  public void close() {
    if (reportStats) {
      statTimer.cancel();
    }
  }

  public void init(Configuration conf) throws WriterException {
  }

  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    CommitStatus rv = ChukwaWriter.COMMIT_OK;
    try {
      Table hbase = connection.getTable(TableName.valueOf(CHUKWA_TABLE));
      Table meta = connection.getTable(TableName.valueOf(CHUKWA_META_TABLE));
      for(Chunk chunk : chunks) {
        synchronized (this) {
          try {
            AbstractProcessor processor = getProcessor(chunk.getDataType());
            processor.process(chunk, output, reporter);
            hbase.put(output);
            meta.put(reporter.getInfo());
          } catch (Throwable e) {
            log.warn(output);
            log.warn(ExceptionUtil.getStackTrace(e));
          }
          dataSize += chunk.getData().length;
          output.clear();
          reporter.clear();
        }
      }
      hbase.close();
      meta.close();
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WriterException("Failed to store data to HBase.");
    }    
    if (next != null) {
      rv = next.add(chunks); //pass data through
    }
    return rv;
  }

  private AbstractProcessor getProcessor(String dataType) throws UnknownRecordTypeException {
    String processorClass = findProcessor(conf.get(dataType, defaultProcessor), defaultProcessor);
    return ProcessorFactory.getProcessor(processorClass);
  }

  /**
   * Look for mapper parser class in the demux configuration.
   * Demux configuration has been changed since CHUKWA-581 to
   * support mapping of both mapper and reducer, and this utility
   * class is to detect the mapper class and return the mapper
   * class only.
   *
   */
  private String findProcessor(String processors, String defaultProcessor) {
    if(processors.startsWith(",")) {
      // No mapper class defined.
      return defaultProcessor;
    } else if(processors.contains(",")) {
      // Both mapper and reducer defined.
      String[] parsers = processors.split(",");
      return parsers[0];
    }
    // No reducer defined.
    return processors;
  }
}
