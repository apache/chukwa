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
package org.apache.hadoop.chukwa.datastore;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.datacollection.writer.hbase.HBaseWriter;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.apache.hadoop.chukwa.util.ExceptionUtil;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class ChukwaHBaseStore {
  private static Configuration hconf = HBaseConfiguration.create();
  private static HTablePool pool = new HTablePool(hconf, 60);
  static Logger log = Logger.getLogger(ChukwaHBaseStore.class);
  
  public static Series getSeries(String tableName, String rkey, String family, String column,
      long startTime, long endTime, boolean filterByRowKey) {
    StringBuilder seriesName = new StringBuilder();
    seriesName.append(rkey);
    seriesName.append(":");
    seriesName.append(family);
    seriesName.append(":");
    seriesName.append(column);

    Series series = new Series(seriesName.toString());
    try {
      HTableInterface table = pool.getTable(tableName);
      Calendar c = Calendar.getInstance();
      c.setTimeInMillis(startTime);
      c.set(Calendar.MINUTE, 0);
      c.set(Calendar.SECOND, 0);
      c.set(Calendar.MILLISECOND, 0);
      String startRow = c.getTimeInMillis()+rkey;
      Scan scan = new Scan();
      scan.addColumn(family.getBytes(), column.getBytes());
      scan.setStartRow(startRow.getBytes());
      scan.setTimeRange(startTime, endTime);
      scan.setMaxVersions();
      if(filterByRowKey) {
        RowFilter rf = new RowFilter(CompareOp.EQUAL, new 
            RegexStringComparator("[0-9]+-"+rkey+"$")); 
        scan.setFilter(rf);
      }
      ResultScanner results = table.getScanner(scan);
      long step = startTime;
      Iterator<Result> it = results.iterator();
      // TODO: Apply discrete wavelet transformation to limit the output
      // size to 1000 data points for graphing optimization. (i.e jwave)
      while(it.hasNext()) {
        Result result = it.next();
        String temp = new String(result.getValue(family.getBytes(), column.getBytes()));
        double value = Double.parseDouble(temp);
        // TODO: Pig Store function does not honor HBase timestamp, hence need to parse rowKey for timestamp.
        String buf = new String(result.getRow());
        Long timestamp = Long.parseLong(buf.split("-")[0]);
        // If Pig Store function can honor HBase timestamp, use the following line is better.
        // series.add(result.getCellValue().getTimestamp(), value);
        series.add(timestamp, value);
      }
      results.close();
      table.close();
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return series;
  }

  public static Set<String> getFamilyNames(String tableName) {
    Set<String> familyNames = new CopyOnWriteArraySet<String>();
    try {
      HTableInterface table = pool.getTable(tableName);
      Calendar c = Calendar.getInstance();
      Set<byte[]> families = table.getTableDescriptor().getFamiliesKeys();
      for(byte[] name : families) {
        familyNames.add(new String(name));
      }
      table.close();
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return familyNames;
    
  }
  
  public static Set<String> getTableNames() {
    Set<String> tableNames = new CopyOnWriteArraySet<String>();
    try {
      HBaseAdmin admin = new HBaseAdmin(hconf);
      HTableDescriptor[] td = admin.listTables();
      for(HTableDescriptor table : td) {
        tableNames.add(new String(table.getName()));
      }
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return tableNames;
  }

  public static void getColumnNamesHelper(Set<String>columnNames, Iterator<Result> it) {
    Result result = it.next();
    if(result!=null) {
      List<KeyValue> kvList = result.list();
      for(KeyValue kv : kvList) {
        columnNames.add(new String(kv.getQualifier()));
      }
    }
  }
  
  public static Set<String> getColumnNames(String tableName, String family, long startTime, long endTime, boolean fullScan) {
    Set<String> columnNames = new CopyOnWriteArraySet<String>();
    try {
      HTableInterface table = pool.getTable(tableName);
      Scan scan = new Scan();
      if(!fullScan) {
        // Take sample columns of the recent time.
        StringBuilder temp = new StringBuilder();
        temp.append(endTime-300000L);
        scan.setStartRow(temp.toString().getBytes());
        temp.setLength(0);
        temp.append(endTime);
        scan.setStopRow(temp.toString().getBytes());
      } else {
        StringBuilder temp = new StringBuilder();
        temp.append(startTime);
        scan.setStartRow(temp.toString().getBytes());
        temp.setLength(0);
        temp.append(endTime);
        scan.setStopRow(temp.toString().getBytes());
      }
      scan.addFamily(family.getBytes());
      ResultScanner results = table.getScanner(scan);
      Iterator<Result> it = results.iterator();
      if(fullScan) {
        while(it.hasNext()) {
          getColumnNamesHelper(columnNames, it);
        }        
      } else {
        getColumnNamesHelper(columnNames, it);        
      }
      results.close();
      table.close();
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return columnNames;
  }
  
  public static Set<String> getRowNames(String tableName, String family, String qualifier, long startTime, long endTime, boolean fullScan) {
    Set<String> rows = new HashSet<String>();
    HTableInterface table = pool.getTable(tableName);
    try {
      Scan scan = new Scan();
      scan.addColumn(family.getBytes(), qualifier.getBytes());
      if(!fullScan) {
        // Take sample columns of the recent time.
        StringBuilder temp = new StringBuilder();
        temp.append(endTime-300000L);
        scan.setStartRow(temp.toString().getBytes());
        temp.setLength(0);
        temp.append(endTime);
        scan.setStopRow(temp.toString().getBytes());
      } else {
        StringBuilder temp = new StringBuilder();
        temp.append(startTime);
        scan.setStartRow(temp.toString().getBytes());
        temp.setLength(0);
        temp.append(endTime);
        scan.setStopRow(temp.toString().getBytes());
      }
      ResultScanner results = table.getScanner(scan);
      Iterator<Result> it = results.iterator();
      while(it.hasNext()) {
        Result result = it.next();
        String buffer = new String(result.getRow());
        String[] parts = buffer.split("-", 2);
        if(!rows.contains(parts[1])) {
          rows.add(parts[1]);
        }    
      }
      results.close();
      table.close();
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return rows;    
  }
  
  public static Set<String> getHostnames(String cluster, long startTime, long endTime, boolean fullScan) {
    return getRowNames("SystemMetrics","system", "csource", startTime, endTime, fullScan);
  }
  
  public static Set<String> getClusterNames(long startTime, long endTime) {
    String tableName = "SystemMetrics";
    String family = "system";
    String column = "ctags";
    Set<String> clusters = new HashSet<String>();
    HTableInterface table = pool.getTable(tableName);
    Pattern p = Pattern.compile("\\s*cluster=\"(.*?)\"");
    try {
      Scan scan = new Scan();
      scan.addColumn(family.getBytes(), column.getBytes());
      scan.setTimeRange(startTime, endTime);
      ResultScanner results = table.getScanner(scan);
      Iterator<Result> it = results.iterator();
      while(it.hasNext()) {
        Result result = it.next();
        String buffer = new String(result.getValue(family.getBytes(), column.getBytes()));
        Matcher m = p.matcher(buffer);
        if(m.matches()) {
          clusters.add(m.group(1));
        }
      }
      results.close();
      table.close();
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return clusters;
  }
}
