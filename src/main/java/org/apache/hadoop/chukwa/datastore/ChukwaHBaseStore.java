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
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.chukwa.hicc.bean.Chart;
import org.apache.hadoop.chukwa.hicc.bean.Dashboard;
import org.apache.hadoop.chukwa.hicc.bean.HeatMapPoint;
import org.apache.hadoop.chukwa.hicc.bean.Heatmap;
import org.apache.hadoop.chukwa.hicc.bean.LineOptions;
import org.apache.hadoop.chukwa.hicc.bean.Series;
import org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData;
import org.apache.hadoop.chukwa.hicc.bean.Widget;
import org.apache.hadoop.chukwa.util.ExceptionUtil;
import org.apache.hadoop.chukwa.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.google.gson.Gson;

public class ChukwaHBaseStore {
  static Logger LOG = Logger.getLogger(ChukwaHBaseStore.class);
  static int MINUTES_IN_HOUR = 60;
  static double RESOLUTION = 360;
  static int MINUTE = 60000; //60 milliseconds
  final static int SECOND = (int) TimeUnit.SECONDS.toMillis(1);
  private final static Charset UTF8 = Charset.forName("UTF-8");

  final static byte[] COLUMN_FAMILY = "t".getBytes(UTF8);
  final static byte[] ANNOTATION_FAMILY = "a".getBytes(UTF8);
  final static byte[] KEY_NAMES = "k".getBytes(UTF8);
  final static byte[] CHART_TYPE = "chart_meta".getBytes(UTF8);
  final static byte[] CHART_FAMILY = "c".getBytes(UTF8);
  final static byte[] COMMON_FAMILY = "c".getBytes(UTF8);
  final static byte[] WIDGET_TYPE = "widget_meta".getBytes(UTF8);
  final static byte[] DASHBOARD_TYPE = "dashboard_meta".getBytes(UTF8);
  private static final String CHUKWA = "chukwa";
  private static final String CHUKWA_META = "chukwa_meta";
  private static long MILLISECONDS_IN_DAY = 86400000L;
  private static Connection connection = null;

  public ChukwaHBaseStore() {
    super();
  }

  public static synchronized void getHBaseConnection() throws IOException {
    if (connection == null || connection.isClosed()) {
      connection = ConnectionFactory.createConnection();
    }
  }
  
  public static synchronized void closeHBase() {
    try {
      if(connection != null) {
        connection.close();
      }
    } catch(IOException e) {
      LOG.warn("Unable to release HBase connection.");
    }
  }
  
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
  public static synchronized Series getSeries(String metric, String source, long startTime,
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

      getHBaseConnection();
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

        scan.addFamily(COLUMN_FAMILY);
        scan.setStartRow(rowKey);
        scan.setStopRow(rowKey);
        scan.setTimeRange(startTime, endTime);
        scan.setBatch(10000);

        ResultScanner results = table.getScanner(scan);
        Iterator<Result> it = results.iterator();

        while (it.hasNext()) {
          Result result = it.next();
          for (Cell kv : result.rawCells()) {
            byte[] key = CellUtil.cloneQualifier(kv);
            long timestamp = ByteBuffer.wrap(key).getLong();
            double value = Double
                .parseDouble(new String(CellUtil.cloneValue(kv), UTF8));
            series.add(timestamp, value);
          }
        }
        results.close();
        currentDay = currentDay + (i * MILLISECONDS_IN_DAY);
      }
      table.close();
    } catch (IOException e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return series;
  }

  public static Set<String> getMetricNames(String metricGroup) {
    Set<String> familyNames = new CopyOnWriteArraySet<String>();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get get = new Get(metricGroup.getBytes(UTF8));
      Result result = table.get(get);
      for (Cell kv : result.rawCells()) {
        JSONObject json = (JSONObject) JSONValue.parse(new String(CellUtil.cloneValue(kv), UTF8));
        if (json.get("type").equals("metric")) {
          familyNames.add(new String(CellUtil.cloneQualifier(kv), UTF8));
        }
      }
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return familyNames;

  }

  public static Set<String> getMetricGroups() {
    Set<String> metricGroups = new CopyOnWriteArraySet<String>();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.addFamily(KEY_NAMES);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while (it.hasNext()) {
        Result result = it.next();
        metricGroups.add(new String(result.getRow(), UTF8));
      }
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return metricGroups;
  }

  public static Set<String> getSourceNames(String dataType) {
    Set<String> pk = new HashSet<String>();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.addFamily(KEY_NAMES);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while (it.hasNext()) {
        Result result = it.next();
        for (Cell cell : result.rawCells()) {
          JSONObject json = (JSONObject) JSONValue.parse(new String(CellUtil.cloneValue(cell), UTF8));
          if (json!=null && json.get("type")!=null && json.get("type").equals("source")) {
            pk.add(new String(CellUtil.cloneQualifier(cell), UTF8));
          }
        }
      }
      rs.close();
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return pk;
  }

  public static Heatmap getHeatmap(String metricGroup, String metric,
      long startTime, long endTime, double max, double scale, int width, int height) {
    Heatmap heatmap = new Heatmap();
    Set<String> sources = getSourceNames(metricGroup);
    Set<String> metrics = getMetricNames(metricGroup);
    List<Get> series = new ArrayList<Get>();
    String fullName = new StringBuilder(metricGroup).append(".").append(metric).toString();
    try {
      getHBaseConnection();
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
      long timeRange = (endTime - startTime);
      Result[] rs = table.get(series);
      int index = 1;
      // Series display in y axis
      int y = 0;
      HashMap<String, Integer> keyMap = new HashMap<String, Integer>();
      for (Result result : rs) {
        for(Cell cell : result.rawCells()) {
          byte[] dest = new byte[5];
          System.arraycopy(CellUtil.cloneRow(cell), 3, dest, 0, 5);
          String source = new String(dest, UTF8);
          long time = cell.getTimestamp();
          // Time display in x axis
          long delta = time - startTime;
          double f = (double) delta / timeRange;
          f = (double) f * width;
          int x = (int) Math.round(f);
          if (keyMap.containsKey(source)) {
            y = keyMap.get(source);
          } else {
            keyMap.put(source, Integer.valueOf(index));
            y = index;
            index++;
          }
          double v = Double.parseDouble(new String(CellUtil.cloneValue(cell), UTF8));
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
      heatmap.putSeries(index -1);
    } catch (IOException e) {
      closeHBase();
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
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.addFamily(KEY_NAMES);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while (it.hasNext()) {
        Result result = it.next();
        for (Cell cell : result.rawCells()) {
          JSONObject json = (JSONObject) JSONValue.parse(new String(CellUtil.cloneValue(cell), UTF8));
          if (json.get("type").equals("cluster")) {
            clusters.add(new String(CellUtil.cloneQualifier(cell), UTF8));
          }
        }
      }
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return clusters;
  }

  /**
   * Get a chart from HBase by ID.
   * 
   * @param id
   * @return
   */
  public static Chart getChart(String id) {
    Chart chart = null;
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get get = new Get(CHART_TYPE);
      Result r = table.get(get);
      byte[] value = r.getValue(CHART_FAMILY, id.getBytes(UTF8));
      Gson gson = new Gson();
      if(value!=null) {
        chart = gson.fromJson(new String(value, UTF8), Chart.class);
      }
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return chart;
  }

  /**
   * Update a chart in HBase by ID.
   * 
   * @param id
   * @param chart
   */
  public static void putChart(String id, Chart chart) {
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Put put = new Put(CHART_TYPE);
      Gson gson = new Gson();
      String buffer = gson.toJson(chart);
      put.addColumn(CHART_FAMILY, id.getBytes(UTF8), buffer.getBytes(UTF8));
      table.put(put);
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    
  }

  /**
   * Create a chart in HBase by specifying parameters.
   * @throws URISyntaxException 
   */
  public static synchronized String createChart(String id, String yunitType, 
      String title, String[] metrics, String source) throws URISyntaxException {
    Chart chart = new Chart(id);
    chart.setYUnitType(yunitType);
    chart.setTitle(title);
    ArrayList<SeriesMetaData> series = new ArrayList<SeriesMetaData>();
    for(String metric : metrics) {
      SeriesMetaData s = new SeriesMetaData();
      s.setLabel(metric + "/" + source);
      s.setUrl(new URI("/hicc/v1/metrics/series/" + metric + "/"
        + source));
      LineOptions l = new LineOptions();
      s.setLineOptions(l);
      series.add(s);
    }
    chart.setSeries(series);
    return createChart(chart);
    
  }

  /**
   * Create a chart in HBase.
   * 
   * @param chart
   * @return id of newly created chart
   * @throws IOException
   */
  public static synchronized String createChart(Chart chart) {
    String id = chart.getId();
    try {
      getHBaseConnection();
      if (id != null) {
        // Check if there is existing chart with same id.
        Chart test = getChart(id);
        if (test != null) {
          // If id already exists, randomly generate an id.
          id = String.valueOf(UUID.randomUUID());
        }
      } else {
        // If id is not provided, randomly generate an id.
        id = String.valueOf(UUID.randomUUID());
      }
      chart.setId(id);
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Put put = new Put(CHART_TYPE);
      Gson gson = new Gson();
      String buffer = gson.toJson(chart);
      put.addColumn(CHART_FAMILY, id.getBytes(UTF8), buffer.getBytes(UTF8));
      table.put(put);
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
      id = null;
    }
    return id;
  }

  /**
   * Return data for multiple series of metrics stored in HBase.
   * 
   * @param series
   * @param startTime
   * @param endTime
   * @return
   */
  public static synchronized ArrayList<org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData> getChartSeries(ArrayList<org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData> series, long startTime, long endTime) {
    ArrayList<org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData> list = new ArrayList<org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData>();
    try {
      // Swap start and end if the values are inverted.
      if (startTime > endTime) {
        long temp = endTime;
        startTime = endTime;
        endTime = temp;
      }
      // Figure out the time range and determine the best resolution
      // to fetch the data
      long range = (endTime - startTime)
        / (long) (MINUTES_IN_HOUR * MINUTE);
      long sampleRate = 1;
      if (range <= 1) {
        sampleRate = 5;
      } else if(range <= 24) {
        sampleRate = 240;
      } else if (range <= 720) {
        sampleRate = 7200;
      } else if(range >= 720) {
        sampleRate = 87600;
      }
      double smoothing = (endTime - startTime)
          / (double) (sampleRate * SECOND ) / (double) RESOLUTION;

      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA));
      Scan scan = new Scan();
      Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
      c.setTimeInMillis(startTime);
      int startDay = c.get(Calendar.DAY_OF_YEAR);
      c.setTimeInMillis(endTime);
      int endDay = c.get(Calendar.DAY_OF_YEAR);
      for (org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData s : series) {
        org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData clone = (org.apache.hadoop.chukwa.hicc.bean.SeriesMetaData) s.clone();
        long currentDay = startTime;
        String[] parts = s.getUrl().toString().split("/");
        String metric = parts[5];
        String source = parts[6];
        ArrayList<ArrayList<Number>> data = new ArrayList<ArrayList<Number>>();
        for (int i = startDay; i <= endDay; i++) {
          byte[] rowKey = HBaseUtil.buildKey(currentDay, metric, source);
          scan.addFamily(COLUMN_FAMILY);
          scan.setStartRow(rowKey);
          scan.setStopRow(rowKey);
          scan.setTimeRange(startTime, endTime);
          scan.setBatch(10000);

          ResultScanner results = table.getScanner(scan);
          Iterator<Result> it = results.iterator();
          double filteredValue = 0.0d;
          long lastTime = startTime;
          long totalElapsedTime = 0;
          int initial = 0;
          
          while (it.hasNext()) {
            Result result = it.next();
            for (Cell kv : result.rawCells()) {
              byte[] key = CellUtil.cloneQualifier(kv);
              long timestamp = ByteBuffer.wrap(key).getLong();
              double value = Double.parseDouble(new String(CellUtil.cloneValue(kv),
                  UTF8));
              if(initial==0) {
                filteredValue = value;
              }
              long elapsedTime = (timestamp - lastTime) / SECOND;
              lastTime = timestamp;
              // Determine if there is any gap, if there is gap in data, reset
              // calculation.
              if (elapsedTime > (sampleRate * 5)) {
                filteredValue = 0.0d;
              } else {
                if (smoothing != 0.0d) {
                  // Apply low pass filter to calculate
                  filteredValue = filteredValue + (double) ((double) elapsedTime * (double) ((double) (value - filteredValue) / smoothing));
                } else {
                  // Use original value
                  filteredValue = value;
                }
              }
              totalElapsedTime = totalElapsedTime + elapsedTime;
              if (totalElapsedTime >= sampleRate) {
                ArrayList<Number> points = new ArrayList<Number>();
                points.add(timestamp);
                points.add(filteredValue);
                data.add(points);
                totalElapsedTime = 0;
              }
            }
            initial++;
          }
          results.close();
          currentDay = currentDay + (i * MILLISECONDS_IN_DAY);
        }
        clone.setData(data);
        list.add(clone);
      }
      table.close();
    } catch (IOException|CloneNotSupportedException e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return list;
  }

  /**
   * List widgets stored in HBase.
   * 
   * @param limit
   * @param offset
   * @return
   */
  public static synchronized List<Widget> listWidget(int limit, int offset) {
    ArrayList<Widget> list = new ArrayList<Widget>();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Scan scan = new Scan();
      scan.setStartRow(WIDGET_TYPE);
      scan.setStopRow(WIDGET_TYPE);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      int c = 0;
      while(it.hasNext()) {
        Result result = it.next();
        for(Cell kv : result.rawCells()) {
          if(c > limit) {
            break;
          }
          if(c < offset) {
            continue;
          }
          Gson gson = new Gson();
          Widget widget = gson.fromJson(new String(CellUtil.cloneValue(kv), UTF8), Widget.class);
          list.add(widget);
          c++;
        }
      }
      rs.close();
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return list;
  }

  /**
   * Find widget by title prefix in HBase.
   * 
   * @param query - Prefix query of widget title.
   * @return
   */
  public static synchronized List<Widget> searchWidget(String query) {
    ArrayList<Widget> list = new ArrayList<Widget>();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Filter filter = new ColumnPrefixFilter(Bytes.toBytes(query));
      Scan scan = new Scan();
      scan.setStartRow(WIDGET_TYPE);
      scan.setStopRow(WIDGET_TYPE);
      scan.setFilter(filter);
      ResultScanner rs = table.getScanner(scan);
      Iterator<Result> it = rs.iterator();
      while(it.hasNext()) {
        Result result = it.next();
        for(Cell kv : result.rawCells()) {
          Gson gson = new Gson();
          Widget widget = gson.fromJson(new String(CellUtil.cloneValue(kv), UTF8), Widget.class);
          list.add(widget);
        }
      }
      rs.close();
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return list;
  }

  /**
   * View a widget information in HBase.
   * 
   * @param title - Title of the widget.
   * @return
   */
  public static synchronized Widget viewWidget(String title) {
    Widget w = null;
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get widget = new Get(WIDGET_TYPE);
      widget.addColumn(COMMON_FAMILY, title.getBytes(UTF8));
      Result rs = table.get(widget);
      byte[] buffer = rs.getValue(COMMON_FAMILY, title.getBytes(UTF8));
      Gson gson = new Gson();
      w = gson.fromJson(new String(buffer, UTF8), Widget.class);
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return w;
  }

  /**
   * Create a widget in HBase.
   * 
   * @param widget
   */
  public static synchronized boolean createWidget(Widget widget) {
    boolean created = false;
    try {
      widget.tokenize();
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get widgetTest = new Get(WIDGET_TYPE);
      widgetTest.addColumn(COMMON_FAMILY, widget.getTitle().getBytes(UTF8));
      if (table.exists(widgetTest)) {
        LOG.warn("Widget: " + widget.getTitle() + " already exists.");
        created = false;
      } else {
        Put put = new Put(WIDGET_TYPE);
        Gson gson = new Gson();
        String buffer = gson.toJson(widget);
        put.addColumn(COMMON_FAMILY, widget.getTitle().getBytes(UTF8), buffer.getBytes(UTF8));
        table.put(put);
        created = true;
      }
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return created;
  }

  /**
   * Update a widget in HBase.
   * 
   * @param title
   * @param widget
   * @throws IOException 
   */
  public static synchronized boolean updateWidget(String title, Widget widget) {
    boolean result = false;
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Delete oldWidget = new Delete(WIDGET_TYPE);
      oldWidget.addColumn(COMMON_FAMILY, title.getBytes(UTF8));
      table.delete(oldWidget);
      Put put = new Put(WIDGET_TYPE);
      Gson gson = new Gson();
      String buffer = gson.toJson(widget);
      put.addColumn(COMMON_FAMILY, title.getBytes(UTF8), buffer.getBytes(UTF8));
      table.put(put);
      table.close();
      result = true;
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
      LOG.error("Error in updating widget, original title: " + 
        title + " new title:" + widget.getTitle());
    }
    return result;
  }

  /**
   * Delete a widget in HBase.
   * 
   * @param title
   * @param widget
   * @throws IOException 
   */
  public static synchronized boolean deleteWidget(String title) {
    boolean result = false;
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Delete oldWidget = new Delete(WIDGET_TYPE);
      oldWidget.addColumn(COMMON_FAMILY, title.getBytes(UTF8));
      table.delete(oldWidget);
      table.close();
      result = true;
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
      LOG.error("Error in deleting widget: "+ title);
    }
    return result;
  }

  public static boolean isDefaultExists() {
    boolean exists = false;
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get dashboardTest = new Get(DASHBOARD_TYPE);
      dashboardTest.addColumn(COMMON_FAMILY, "default".getBytes(UTF8));
      exists = table.exists(dashboardTest);
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
    }
    return exists;
  }

  public static void populateDefaults() {
    boolean defaultExists = isDefaultExists();
    try {
      if(defaultExists) {
        return;
      }
      String hostname = InetAddress.getLocalHost().getHostName();
      // Populate example chart widgets
      String[] metrics = { "SystemMetrics.LoadAverage.1" };
      createChart("1", "", "System Load Average", metrics, hostname);
      String[] cpuMetrics = { "SystemMetrics.cpu.combined", "SystemMetrics.cpu.sys", "SystemMetrics.cpu.user" };
      createChart("2", "percent", "CPU Utilization", cpuMetrics, hostname);
      String[] memMetrics = { "SystemMetrics.memory.FreePercent", "SystemMetrics.memory.UsedPercent"};
      createChart("3", "percent", "Memory Utilization", memMetrics, hostname);
      String[] diskMetrics = { "SystemMetrics.disk.ReadBytes", "SystemMetrics.disk.WriteBytes" };
      createChart("4", "bytes-decimal", "Disk Utilization", diskMetrics, hostname);
      String[] netMetrics = { "SystemMetrics.network.TxBytes", "SystemMetrics.network.RxBytes" };
      createChart("5", "bytes", "Network Utilization", netMetrics, hostname);
      String[] swapMetrics = { "SystemMetrics.swap.Total", "SystemMetrics.swap.Used", "SystemMetrics.swap.Free" };
      createChart("6", "bytes-decimal", "Swap Utilization", swapMetrics, hostname);
      
      // Populate default widgets
      Widget widget = new Widget();
      widget.setTitle("System Load Average");
      widget.setSrc(new URI("/hicc/v1/chart/draw/1"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(2);
      widget.setSize_y(1);
      createWidget(widget);

      // CPU heatmap
      widget = new Widget();
      widget.setTitle("CPU Heatmap");
      widget.setSrc(new URI("/hicc/jsp/heatmap.jsp"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(5);
      widget.setSize_y(1);
      createWidget(widget);

      Dashboard dashboard = new Dashboard();
      // Log Search widget
      widget = new Widget();
      widget.setTitle("Log Search");
      widget.setSrc(new URI("/hicc/ajax-solr/chukwa"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(7);
      widget.setSize_y(4);
      createWidget(widget);
      dashboard.add(widget);
      updateDashboard("default", "", dashboard);

      // Populate system dashboards
      dashboard = new Dashboard();
      widget = new Widget();
      widget.setTitle("CPU Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/2"));
      widget.setCol(1);
      widget.setRow(1);
      widget.setSize_x(3);
      widget.setSize_y(1);
      createWidget(widget);
      dashboard.add(widget);

      widget = new Widget();
      widget.setTitle("Memory Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/3"));
      widget.setCol(4);
      widget.setRow(1);
      widget.setSize_x(3);
      widget.setSize_y(1);
      createWidget(widget);
      dashboard.add(widget);

      widget = new Widget();
      widget.setTitle("Disk Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/4"));
      widget.setCol(1);
      widget.setRow(2);
      widget.setSize_x(3);
      widget.setSize_y(1);
      createWidget(widget);
      dashboard.add(widget);

      widget = new Widget();
      widget.setTitle("Network Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/5"));
      widget.setCol(4);
      widget.setRow(2);
      widget.setSize_x(3);
      widget.setSize_y(1);
      createWidget(widget);
      dashboard.add(widget);

      widget = new Widget();
      widget.setTitle("Swap Utilization");
      widget.setSrc(new URI("/hicc/v1/chart/draw/6"));
      widget.setCol(1);
      widget.setRow(3);
      widget.setSize_x(3);
      widget.setSize_y(1);
      createWidget(widget);
      dashboard.add(widget);

      widget = new Widget();
      widget.setTitle("System Load Average");
      widget.setSrc(new URI("/hicc/v1/chart/draw/1"));
      widget.setCol(4);
      widget.setRow(3);
      widget.setSize_x(3);
      widget.setSize_y(1);
      createWidget(widget);
      dashboard.add(widget);

      updateDashboard("system", "", dashboard);
      
    } catch (Throwable ex) {
      LOG.error(ExceptionUtil.getStackTrace(ex));
    }
  }

  public static synchronized Dashboard getDashboard(String id, String user) {
    Dashboard dash = null;
    String key = new StringBuilder().append(id).
        append("|").append(user).toString();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Get dashboard = new Get(DASHBOARD_TYPE);
      dashboard.addColumn(COMMON_FAMILY, key.getBytes(UTF8));
      Result rs = table.get(dashboard);
      byte[] buffer = rs.getValue(COMMON_FAMILY, key.getBytes(UTF8));
      if(buffer == null) {
        // If user dashboard is not found, use default dashboard.
        key = new StringBuilder().append(id).append("|").toString();
        dashboard = new Get(DASHBOARD_TYPE);
        dashboard.addColumn(COMMON_FAMILY, key.getBytes(UTF8));
        rs = table.get(dashboard);
        buffer = rs.getValue(COMMON_FAMILY, key.getBytes(UTF8));        
      }
      Gson gson = new Gson();
      dash = gson.fromJson(new String(buffer, UTF8), Dashboard.class);
      table.close();
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
      LOG.error("Error retrieving dashboard, id: " + 
        id + " user:" + user);
    }
    return dash;
  }

  public static boolean updateDashboard(String id, String user, Dashboard dash) {
    boolean result = false;
    String key = new StringBuilder().append(id).
        append("|").append(user).toString();
    try {
      getHBaseConnection();
      Table table = connection.getTable(TableName.valueOf(CHUKWA_META));
      Put put = new Put(DASHBOARD_TYPE);
      Gson gson = new Gson();
      String buffer = gson.toJson(dash);
      put.addColumn(COMMON_FAMILY, key.getBytes(UTF8), buffer.getBytes(UTF8));
      table.put(put);
      table.close();
      result = true;
    } catch (Exception e) {
      closeHBase();
      LOG.error(ExceptionUtil.getStackTrace(e));
      LOG.error("Error in updating dashboard, id: " + 
        id + " user:" + user);
    }
    return result;
  }

}
