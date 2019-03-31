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

package org.apache.hadoop.chukwa.hicc.bean;


import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class Chart {
  private String id;
  private ChartType type;
  private String title;
  private List<SeriesMetaData> series;
  private boolean xLabelOn;
  private boolean yLabelOn;
  private boolean yRightLabelOn;
  private int width;
  private int height;
  private List<String> xLabelRange;
  private HashMap<String, Long> xLabelRangeHash;
  private boolean legend = true;
  private String xLabel = "";
  private String yLabel = "";
  private String yRightLabel = "";
  private double max = 0;
  private double min = 0;
  private boolean userDefinedMax = true;
  private boolean userDefinedMin = true;
  private String yUnitType = "";
  private String icon = "";
  private String bannerText = "";
  private String suffixText = "";
  private String threshold = "";

  public Chart(String id) {
    this.id = id;
    this.type = ChartType.TIME_SERIES;
    this.title = "Untitled Chart";
    this.xLabelOn = true;
    this.yLabelOn = true;
    this.width = 100;
    this.height = 100;
    this.legend = true;
    this.max = 0;
    this.userDefinedMax = false;
    this.userDefinedMin = false;
  }

  public ChartType getType() {
    return this.type;
  }
  
  public void setType(ChartType type) {
    this.type = type;
  }

  public void setYMax(double max) {
    this.max = max;
    this.userDefinedMax = true;
  }

  public double getYMax() {
    return this.max;
  }

  public boolean getUserDefinedMax() {
    return this.userDefinedMax;
  }

  public void setYMin(double min) {
    this.min = min;
    this.userDefinedMin = true;
  }

  public double getYMin() {
    return this.min;
  }

  public boolean getUserDefinedMin() {
    return this.userDefinedMin;
  }

  public void setSize(int width, int height) {
    this.width = width;
    this.height = height;
  }

  public int getWidth() {
    return this.width;
  }

  public int getHeight() {
    return this.height;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public String getTitle() {
    return this.title;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return this.id;
  }

  public void setSeries(List<SeriesMetaData> series) {
    this.series = series;
  }
  
  public List<SeriesMetaData> getSeries() {
    return this.series;
  }
  
  public void setXAxisLabelsOn(boolean toggle) {
    xLabelOn = toggle;
  }

  public boolean isXAxisLabelsOn() {
    return xLabelOn;
  }

  public void setYAxisLabels(boolean toggle) {
    yLabelOn = toggle;
  }

  public boolean isYAxisLabelsOn() {
    return yLabelOn;
  }

  public void setYAxisRightLabels(boolean toggle) {
    yRightLabelOn = toggle;
  }

  public boolean isYAxisRightLabelsOn() {
    return yRightLabelOn;
  }

  public void setXAxisLabel(String label) {
    xLabel = label;
  }

  public String getXAxisLabel() {
    return xLabel;
  }

  public void setYAxisLabel(String label) {
    yLabel = label;
  }

  public String getYAxisLabel() {
    return yLabel;
  }

  public void setYAxisRightLabel(String label) {
    yRightLabel = label;
  }

  public String getYAxisRightLabel() {
    return yRightLabel;
  }

  public void setXLabelsRange(List<String> range) {
    xLabelRange = range;
    xLabelRangeHash = new HashMap<String, Long>();
    long value = 0;
    for (String label : range) {
      xLabelRangeHash.put(label, value);
      value++;
    }
  }

  public List<String> getXLabelsRange() {
    return xLabelRange;
  }
  
  public void setLegend(boolean toggle) {
    legend = toggle;
  }

  public boolean getLegend() {
    return legend;
  }

  public void setYUnitType(String yUnitType) {
    this.yUnitType = yUnitType;
  }
  
  public String getYUnitType() {
    return this.yUnitType;
  }

  public void setIcon(String icon) {
    this.icon = icon;
  }

  public String getIcon() {
    return this.icon;
  }

  public void setBannerText(String bannerText) {
    this.bannerText = bannerText;
  }

  public String getBannerText() {
    return this.bannerText;
  }

  public void setSuffixText(String suffixText) {
    this.suffixText = suffixText;
  }

  public String getSuffixText() {
    return this.suffixText;
  }

  public void setThreshold(String direction) {
    this.threshold = direction;
  }
  
  public String getThreshold() {
    return this.threshold;
  }

  /**
   * Create a chart object.
   * @param id is unique chart identifier
   * @param title is searchable name of the chart
   * @param metrics is list of metric names to render chart
   * @param source is data source name
   * @param yunitType is y axis unit type
   * @return Chart object
   * @throws URISyntaxException if metrics name can not compose valid URL syntax
   */
  public static synchronized Chart createChart(String id,
      String title, String[] metrics, String source, String yunitType) throws URISyntaxException {
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
    return chart;
    
  }
  
  /**
   * Create a chart in HBase by specifying parameters.
   * @param id is unique chart identifier
   * @param title is searchable name of the chart
   * @param metrics is list of metric names to render ring chart
   * @param source is data source name
   * @param suffixLabel is text label to append to metric values
   * @param direction sets the threshold to have either upper limit or lower limit
   * @return Chart object
   * @throws URISyntaxException if metrics name can not compose valid URL syntax
   */
  public static synchronized Chart createCircle(String id,
      String title, String[] metrics, String source, String suffixLabel, String direction) throws URISyntaxException {
    Chart chart = new Chart(id);
    chart.setSuffixText(suffixLabel);
    chart.setTitle(title);
    chart.setThreshold(direction);
    ArrayList<SeriesMetaData> series = new ArrayList<SeriesMetaData>();
    for(String metric : metrics) {
      SeriesMetaData s = new SeriesMetaData();
      s.setLabel(metric + "/" + source);
      s.setUrl(new URI("/hicc/v1/metrics/series/" + metric + "/"
        + source));
      series.add(s);
    }
    chart.setSeries(series);
    return chart;
    
  }
  
  /**
   * Create a tile in HBase by specifying parameters.
   * @param id is unique tile identifier
   * @param title is searchable name of the tile widget
   * @param bannerText is description of the tile widget
   * @param suffixLabel is text label to append to metric values
   * @param metrics is list of metric names to render tile widget
   * @param source is data source name
   * @param icon is emoji symbol to render beside tile widget
   * @return Chart object
   * @throws URISyntaxException if metrics name can not compose valid URL syntax
   */
  public static synchronized Chart createTile(String id, String title, 
      String bannerText, String suffixLabel, String[] metrics, String source, 
      String icon) throws URISyntaxException {
    Chart chart = new Chart(id);
    chart.setTitle(title);
    chart.setBannerText(bannerText);
    chart.setSuffixText(suffixLabel);
    chart.setIcon(icon);
    List<SeriesMetaData> smd = new ArrayList<SeriesMetaData>();
    for (String metric : metrics) {
      SeriesMetaData series = new SeriesMetaData();
      series.setUrl(new URI("/hicc/v1/metrics/series/" + metric + "/" + source));
      smd.add(series);
    }
    chart.setSeries(smd);
    return chart;
  }
}
