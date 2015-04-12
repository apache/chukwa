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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.chukwa.hicc.bean.HeatMapPoint;
import org.apache.hadoop.chukwa.hicc.bean.Heatmap;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

public class ChukwaHBaseStore {
  private static Configuration hconf = HBaseConfiguration.create();
  static Logger LOG = Logger.getLogger(ChukwaHBaseStore.class);
  static byte[] COLUMN_FAMILY = "t".getBytes();
  static byte[] ANNOTATION_FAMILY = "a".getBytes();
  static byte[] KEY_NAMES = "k".getBytes();
  private static final String CHUKWA = "chukwa";
  private static final String CHUKWA_META = "chukwa_meta";
  private static long MILLISECONDS_IN_DAY = 86400000L;

  /**
   * Scan chukwa table for a particular metric group and metric name based on
   * time ranges.
   * 
   * @param metricGroup
   * @param metric
   * @param source
   * @param startTime
   * @param endTime
   * @return
   */
  public static Series getSeries(String metricGroup, String metric,
      String source, long startTime, long endTime) {
    String fullMetricName = new StringBuilder(metricGroup).append(".")
        .append(metric).toString();
    return getSeries(fullMetricName, source, startTime, endTime);
  }

  /**
   * Scan chukwa table for a full metric name based on time ranges.
   * 
   * @param metric
   * @param source
   * @param startTime
   * @param endTime
   * @return
   */
  public static Series getSeries(String metric, String source, long startTime,
      long endTime) {
    String seriesName = new StringBuilder(metric).append(":").append(source).toString();
    Series series = new Series(seriesName);
    try {
      // Swap start and end if the values are inverted.
      if (startTime > endTime) {
        long temp = endTime;
        startTime = endTime;
        endTime = temp;
      }
      Connection connection = ConnectionFactory.createConnection(hconf);
      Table table = connection.getTable(TableName.valueOf(CHUKWA));
      Scan scan = new Scan();
      Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      c.setTimeInMillis(startTime);
      int startDay = c.get(Calendar.DAY_OF_YEAR);
      c.setTimeInMillis(endTime);
      int endDay = c.get(Calendar.DAY_OF_YEAR);
      long currentDay = startTime;
      for (int i = startDay; i <= endDay; i++) {
        byte[] rowKey = HBaseUtil.buildKey(currentDay, metric, source);
        // ColumnRangeFilter crf = new
        // ColumnRangeFilter(Long.valueOf(startTime).toString().getBytes(),
        // true, Long.valueOf(endTime).toString().getBytes(), true);
        // scan.setFilter(crf);
        scan.addFamily(COLUMN_FAMILY);
        scan.setStartRow(rowKey);
        scan.setStopRow(rowKey);
        scan.setTimeRange(startTime, endTime);
        scan.setBatch(10000);

        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();
        // TODO: Apply discrete wavelet transformation to limit the output
        // size to 1000 data points for graphing optimization. (i.e jwave)
        while (it.hasNext()) {
          Result result = it.next();
          for (KeyValue kv : result.raw()) {
            byte[] key = kv.getQualifier();
            long timestamp = ByteBuffer.wrap(key).getLong();
            double value = Double
                .parseDouble(new String(kv.getValue(), "UTF-8"));
            series.add(timestamp, value);
          }
        }
        results.close();
        currentDay = currentDay + (i * MILLISECONDS_IN_DAY);
      }
      table.close();
    } catch (Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return series;
  }

  public static Set<String> getMetricNames(String metricGroup) {
    Set<String> familyNames = new CopyOnWriteArraySet<String>();
    try {
      Connection connection = ConnectionFactory.createConnection(hconf);
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get get = new Get(metricGroup.getBytes());
      Result result = table.get(get);
      for (KeyValue kv : result.raw()) {
        JSONObject json = (JSONObject) JSONValue.parse(new String(kv.getValue(), "UTF-8"));
        if (json.get("type").equals("metric")) {
          familyNames.add(new String(kv.getQualifier(), "UTF-8"));
        }
      }
      table.close();
      connection.close();
    } catch (Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return familyNames;

  }

  public static Set<String> getMetricGroups() {
    Set<String> metricGroups = new CopyOnWriteArraySet<String>();
    try {
      Connection connection = ConnectionFactory.createConnection(hconf);
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.addFamily(KEY_NAMES);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while (it.hasNext()) {
        Result result = it.next();
        metricGroups.add(new String(result.getRow(), "UTF-8"));
      }
      table.close();
      connection.close();
    } catch (Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return metricGroups;
  }

  public static Set<String> getSourceNames(String dataType) {
    Set<String> pk = new HashSet<String>();
    try {
      Connection connection = ConnectionFactory.createConnection(hconf);
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.addFamily(KEY_NAMES);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while (it.hasNext()) {
        Result result = it.next();
        for (Cell cell : result.rawCells()) {
          JSONObject json = (JSONObject) JSONValue.parse(new String(cell.getValue(), "UTF-8"));
          if (json.get("type").equals("source")) {
            pk.add(new String(cell.getQualifier(), "UTF-8"));
          }
        }
      }
      table.close();
      connection.close();
    } catch (Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return pk;
  }

  public static Heatmap getHeatmap(String metricGroup, String metric,
      long startTime, long endTime, double max, double scale, int height) {
    final long MINUTE = TimeUnit.MINUTES.toMillis(1);
    Heatmap heatmap = new Heatmap();
    Set<String> sources = getSourceNames(metricGroup);
    Set<String> metrics = getMetricNames(metricGroup);
    List<Get> series = new ArrayList<Get>();
    String fullName = new StringBuilder(metricGroup).append(".").append(metric).toString();
    try {
      Connection connection = ConnectionFactory.createConnection(hconf);
      Table table = connection.getTable(TableName.valueOf(CHUKWA));
      Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      c.setTimeInMillis(startTime);
      int startDay = c.get(Calendar.DAY_OF_YEAR);
      c.setTimeInMillis(endTime);
      int endDay = c.get(Calendar.DAY_OF_YEAR);
      long currentDay = startTime;
      for (int i = startDay; i <= endDay; i++) {
        for (String m : metrics) {
          if (m.startsWith(fullName)) {
            for (String source : sources) {
              byte[] rowKey = HBaseUtil.buildKey(currentDay, m, source);
              Get serie = new Get(rowKey);
              serie.addFamily(COLUMN_FAMILY);
              serie.setTimeRange(startTime, endTime);
              series.add(serie);
            }
          }
        }
        currentDay = currentDay + (i * MILLISECONDS_IN_DAY);
      }
      Result[] rs = table.get(series);
      int index = 0;
      // Series display in y axis
      int y = 0;
      HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
      for (Result result : rs) {
        for(Cell cell : result.rawCells()) {
          byte[] dest = new byte[5];
          System.arraycopy(cell.getRow(), 3, dest, 0, 5);
          String source = new String(dest);
          long time = cell.getTimestamp();
          // Time display in x axis
          int x = (int) ((time - startTime) / MINUTE);
          if (keyMap.containsKey(source)) {
            y = keyMap.get(source);
          } else {
            keyMap.put(source, new Integer(index));
            y = index;
            index++;
          }
          double v = Double.parseDouble(new String(CellUtil.cloneValue(cell)));
          heatmap.put(x, y, v);
          if (v > max) {
            max = v;
          }
        }
      }
      table.close();
      int radius = height / index;
      // Usually scale max from 0 to 100 for visualization
      heatmap.putMax(scale);
      for (HeatMapPoint point : heatmap.getHeatmap()) {
        double round = point.count / max * scale;
        round = Math.round(round * 100.0) / 100.0;
        point.put(point.x, point.y * radius, round);
      }
      heatmap.putRadius(radius);
      heatmap.putSeries(index);
    } catch (IOException e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return heatmap;
  }

  /**
   * Scan chukwa table and find cluster tag from annotation column family from a
   * range of entries.
   * 
   * @param startTime
   * @param endTime
   * @return
   */
  public static Set<String> getClusterNames(long startTime, long endTime) {
    Set<String> clusters = new HashSet<String>();
    try {
      Connection connection = ConnectionFactory.createConnection(hconf);
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.addFamily(KEY_NAMES);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while (it.hasNext()) {
        Result result = it.next();
        for (Cell cell : result.rawCells()) {
          JSONObject json = (JSONObject) JSONValue.parse(new String(cell.getValue(), "UTF-8"));
          if (json.get("type").equals("cluster")) {
            clusters.add(new String(cell.getQualifier(), "UTF-8"));
          }
        }
      }
      table.close();
      connection.close();
    } catch (Exception e) {
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return clusters;
  }

}
