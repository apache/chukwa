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
package org.apache.hadoop.chukwa.hicc;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Series implements Cloneable {

  @XmlElement
  public URI url;
  @XmlElement
  public String color;
  @XmlElement
  public String label;
  @XmlElement
  public LineOptions lines;
  @XmlElement
  public BarOptions bars;
  @XmlElement
  public PointOptions points;
  @XmlElement
  public int xaxis;
  @XmlElement
  public int yaxis;
  @XmlElement
  public boolean clickable;
  @XmlElement
  public boolean hoverable;
  @XmlElement
  public int shadowSize;
  @XmlElement
  public int highlightColor;
  public ArrayList<ArrayList<Number>> data = null;
  
  public Series() {
    
  }

  public void setUrl(URI url) {
    this.url = url;
  }
  
  public URI getUrl() {
    return url;
  }
  
  public void setLineOptions(LineOptions lines) {
    this.lines = lines;
    
  }
  
  public LineOptions getLineOptions() {
    return lines;
  }
  
  public void setBarOptions(BarOptions bars) {
    this.bars = bars;
  }
  
  public BarOptions getBarOptions() {
    return bars;
  }
  
  public void setPointOptions(PointOptions points) {
    this.points = points;
  }
  
  public PointOptions getPointOptions() {
    return points;
  }
  
  public void setColor(String color) {
    this.color = color;
  }
  
  public String getColor() {
    return color;
  }

  public void setData(ArrayList<ArrayList<Number>> data) {
    this.data = data;
  }
  
  public ArrayList<ArrayList<Number>> getData() {
    return data;
  }

  public void setLabel(String label) {
    this.label = label;
  }
  
  public String getLabel() {
    return label;
  }

  @Override
  public Object clone()throws CloneNotSupportedException{  
    return super.clone();  
  }
}
