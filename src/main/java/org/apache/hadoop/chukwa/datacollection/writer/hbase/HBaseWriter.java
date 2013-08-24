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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.chukwa.ChukwaArchiveKey;
import org.apache.hadoop.chukwa.Chunk;
import org.apache.hadoop.chukwa.conf.ChukwaConfiguration;
import org.apache.hadoop.chukwa.datacollection.writer.ChukwaWriter;
import org.apache.hadoop.chukwa.datacollection.writer.PipelineableWriter;
import org.apache.hadoop.chukwa.datacollection.writer.WriterException;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessor;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.MapProcessorFactory;
import org.apache.hadoop.chukwa.extraction.demux.processor.mapper.UnknownRecordTypeException;
import org.apache.hadoop.chukwa.extraction.demux.Demux;
import org.apache.hadoop.chukwa.util.ClassUtils;
import org.apache.hadoop.chukwa.util.DaemonWatcher;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Table;
import org.apache.hadoop.chukwa.datacollection.writer.hbase.Annotation.Tables;
import org.apache.log4j.Logger;

public class HBaseWriter extends PipelineableWriter {
  static Logger log = Logger.getLogger(HBaseWriter.class);
  boolean reportStats;
  volatile long dataSize = 0;
  final Timer statTimer;
  private OutputCollector output;
  private Reporter reporter;
  private ChukwaConfiguration conf;
  String defaultProcessor;
  private HTablePool pool;
  private Configuration hconf;
  
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

  public HBaseWriter() {
    this(true);
  }

  public HBaseWriter(boolean reportStats) {
    /* HBase Version >= 0.89.x */
    this(reportStats, new ChukwaConfiguration(), HBaseConfiguration.create());
  }

  public HBaseWriter(ChukwaConfiguration conf, Configuration hconf) {
    this(true, conf, hconf);
  }

  private HBaseWriter(boolean reportStats, ChukwaConfiguration conf, Configuration hconf) {
    this.reportStats = reportStats;
    this.conf = conf;
    this.hconf = hconf;
    this.statTimer = new Timer();
    this.defaultProcessor = conf.get(
      "chukwa.demux.mapper.default.processor",
      "org.apache.hadoop.chukwa.extraction.demux.processor.mapper.DefaultProcessor");
    Demux.jobConf = conf;
    log.info("hbase.zookeeper.quorum: " + hconf.get("hbase.zookeeper.quorum"));
  }

  public void close() {
    if (reportStats) {
      statTimer.cancel();
    }
  }

  public void init(Configuration conf) throws WriterException {
    if (reportStats) {
      statTimer.schedule(new StatReportingTask(), 1000, 10 * 1000);
    }
    output = new OutputCollector();
    reporter = new Reporter();
    if(conf.getBoolean("hbase.writer.verify.schema", false)) {
      verifyHbaseSchema();      
    }
    pool = new HTablePool(hconf, 60);
  }

  private boolean verifyHbaseTable(HBaseAdmin admin, Table table) {
    boolean status = false;
    try {
      if(admin.tableExists(table.name())) {
        HTableDescriptor descriptor = admin.getTableDescriptor(table.name().getBytes());
        HColumnDescriptor[] columnDescriptors = descriptor.getColumnFamilies();
        for(HColumnDescriptor cd : columnDescriptors) {
          if(cd.getNameAsString().equals(table.columnFamily())) {
            log.info("Verified schema - table: "+table.name()+" column family: "+table.columnFamily());
            status = true;
          }
        }
      } else {
        throw new Exception("HBase table: "+table.name()+ " does not exist.");
      }
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      status = false;
    }
    return status;    
  }
  
  private void verifyHbaseSchema() {
    log.debug("Verify Demux parser with HBase schema");
    boolean schemaVerified = true;
    try {
      HBaseAdmin admin = new HBaseAdmin(hconf);
      List<Class> demuxParsers = ClassUtils.getClassesForPackage(conf.get("hbase.demux.package"));
      for(Class<?> x : demuxParsers) {
        if(x.isAnnotationPresent(Tables.class)) {
          Tables list = x.getAnnotation(Tables.class);
          for(Table table : list.annotations()) {
            if(!verifyHbaseTable(admin, table)) {
              schemaVerified = false;
              log.warn("Validation failed - table: "+table.name()+" column family: "+table.columnFamily()+" does not exist.");              
            }
          }
        } else if(x.isAnnotationPresent(Table.class)) {
          Table table = x.getAnnotation(Table.class);
          if(!verifyHbaseTable(admin, table)) {
            schemaVerified = false;
            log.warn("Validation failed - table: "+table.name()+" column family: "+table.columnFamily()+" does not exist.");
          }
        }
      }
    } catch (Exception e) {
      schemaVerified = false;
      log.error(ExceptionUtil.getStackTrace(e));
    }
    if(!schemaVerified) {
      log.error("Hbase schema mismatch with demux parser.");
      if(conf.getBoolean("hbase.writer.halt.on.schema.mismatch", true)) {
        log.error("Exiting...");
        DaemonWatcher.bailout(-1);
      }
    }
  }

  @Override
  public CommitStatus add(List<Chunk> chunks) throws WriterException {
    CommitStatus rv = ChukwaWriter.COMMIT_OK;
    try {
      for(Chunk chunk : chunks) {
        synchronized (this) {
          try {
            Table table = findHBaseTable(chunk.getDataType());

            if(table!=null) {
              HTableInterface hbase = pool.getTable(table.name().getBytes());
              MapProcessor processor = getProcessor(chunk.getDataType());
              processor.process(new ChukwaArchiveKey(), chunk, output, reporter);

              hbase.put(output.getKeyValues());
              pool.putTable(hbase);
            }
          } catch (Exception e) {
            log.warn(output.getKeyValues());
            log.warn(ExceptionUtil.getStackTrace(e));
          }
          dataSize += chunk.getData().length;
          output.clear();
          reporter.clear();
        }
      }
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
      throw new WriterException("Failed to store data to HBase.");
    }    
    if (next != null) {
      rv = next.add(chunks); //pass data through
    }
    return rv;
  }

  public Table findHBaseTable(String dataType) throws UnknownRecordTypeException {
    MapProcessor processor = getProcessor(dataType);

    Table table = null;
    if(processor.getClass().isAnnotationPresent(Table.class)) {
      return processor.getClass().getAnnotation(Table.class);
    } else if(processor.getClass().isAnnotationPresent(Tables.class)) {
      Tables tables = processor.getClass().getAnnotation(Tables.class);
      for(Table t : tables.annotations()) {
        table = t;
      }
    }

    return table;
  }

  public String findHBaseColumnFamilyName(String dataType)
          throws UnknownRecordTypeException {
    Table table = findHBaseTable(dataType);
    return table.columnFamily();
  }

  private MapProcessor getProcessor(String dataType) throws UnknownRecordTypeException {
    String processorClass = findProcessor(conf.get(dataType, defaultProcessor), defaultProcessor);
    return MapProcessorFactory.getProcessor(processorClass);
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
