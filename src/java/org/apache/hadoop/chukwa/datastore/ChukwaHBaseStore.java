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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;

public class ChukwaHBaseStore {
  private static HBaseConfiguration hconf = hconf = new HBaseConfiguration();
  private static HTablePool pool = new HTablePool(hconf, 60);
  static Logger log = Logger.getLogger(ChukwaHBaseStore.class);
  
  public static Series getSeries(String tableName, String rkey, String column,
      long startTime, long endTime, boolean filterByRowKey) {
    StringBuilder seriesName = new StringBuilder();
    seriesName.append(rkey);
    seriesName.append(":");
    seriesName.append(column);
    Series series = new Series(seriesName.toString());
    try {
      HTable table = pool.getTable(tableName);
      Calendar c = Calendar.getInstance();
      c.setTimeInMillis(startTime);
      c.set(Calendar.MINUTE, 0);
      c.set(Calendar.SECOND, 0);
      c.set(Calendar.MILLISECOND, 0);
      String startRow = c.getTimeInMillis()+rkey;
      Scan scan = new Scan();
      scan.addColumn(column.getBytes());
      scan.setStartRow(startRow.getBytes());
      scan.setTimeRange(startTime, endTime);
      scan.setMaxVersions();
      if(filterByRowKey) {
        RowFilter rf = new RowFilter(CompareOp.EQUAL, new 
            SubstringComparator(rkey)); 
        scan.setFilter(rf);
      }
      ResultScanner results = table.getScanner(scan);
      long step = startTime;
      Iterator<Result> it = results.iterator();
      // TODO: Apply discrete wavelet transformation to limit the output
      // size to 1000 data points for graphing optimization. (i.e jwave)
      while(it.hasNext()) {
        Result result = it.next();
        String temp = new String(result.getCellValue().getValue());
        double value = Double.parseDouble(temp);
        series.add(result.getCellValue().getTimestamp(), value);
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
      HTable table = pool.getTable(tableName);
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
  
  public static Set<String> getColumnNames(String tableName, String family, long startTime, long endTime) {
    Set<String> columnNames = new CopyOnWriteArraySet<String>();
    try {
      HTable table = pool.getTable(tableName);
      Scan scan = new Scan();
      scan.setTimeRange(startTime, endTime);
      scan.addFamily(family.getBytes());
      ResultScanner results = table.getScanner(scan);
      Iterator<Result> it = results.iterator();
      while(it.hasNext()) {
        Result result = it.next();
        List<KeyValue> kvList = result.list();
        for(KeyValue kv : kvList) {
          columnNames.add(new String(kv.getColumn()));
        }
      }
      results.close();
      table.close();
    } catch(Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return columnNames;
  }
  
  public static Set<String> getRowNames(String tableName, String column, long startTime, long endTime) {
    Set<String> rows = new HashSet<String>();
    HTable table = pool.getTable(tableName);
    try {
      Scan scan = new Scan();
      scan.addColumn(column.getBytes());
      scan.setTimeRange(startTime, endTime);
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
  
  public static Set<String> getHostnames(String cluster, long startTime, long endTime) {
    return getRowNames("SystemMetrics","system:csource", startTime, endTime);
  }
  
  public static Set<String> getClusterNames(long startTime, long endTime) {
    String tableName = "SystemMetrics";
    String column = "system:ctags";
    Set<String> clusters = new HashSet<String>();
    HTable table = pool.getTable(tableName);
    Pattern p = Pattern.compile("\\s*cluster=\"(.*?)\"");
    try {
      Scan scan = new Scan();
      scan.addColumn(column.getBytes());
      scan.setTimeRange(startTime, endTime);
      ResultScanner results = table.getScanner(scan);
      Iterator<Result> it = results.iterator();
      while(it.hasNext()) {
        Result result = it.next();
        String buffer = new String(result.getValue(column.getBytes()));
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
