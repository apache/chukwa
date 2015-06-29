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


import java.util.HashMap;
import java.util.List;

public class Chart {
  private String id;
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

  public Chart(String id) {
    this.id = id;
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
}
