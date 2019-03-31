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

import java.util.ArrayList;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(propOrder={})
public class Heatmap {
  @XmlElement
  private ArrayList<HeatMapPoint> data;
  @XmlElement
  private double max = 1.0;
  @XmlElement
  private int radius;
  @XmlElement
  private int series;
  
  public Heatmap() {
	  this.data = new ArrayList<HeatMapPoint>();
  }
  
  public void put(int x, int y, double v) {
	  HeatMapPoint point = new HeatMapPoint(x, y, v);
	  data.add(point);
  }
  
  public ArrayList<HeatMapPoint> getHeatmap() {
	  return data;
  }
  
  public double getMax() {
	  return max;
  }
  
  public void putMax(double max) {
	  this.max = max;
  }

  public int getRadius() {
	  return radius;
  }
  
  public void putRadius(int radius) {
	  this.radius = radius;
  }
  
  public int getSeries() {
	  return series;
  }
  
  public void putSeries(int series) {
	  this.series = series;
  }
}
