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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Widget {
  @XmlElement
  public int col;
  @XmlElement
  public int row;
  @XmlElement
  public int size_x;
  @XmlElement
  public int size_y;
  @XmlElement
  public String title;
  @XmlElement
  public URI src;
  @XmlElement
  public String[] tokens;

  public int getCol() {
    return col;
  }

  public void setCol(int col) {
    this.col = col;
  }

  public int getRow() {
    return row;
  }

  public void setRow(int row) {
    this.row = row;
  }

  public int getSize_x() {
    return size_x;
  }

  public void setSize_x(int size_x) {
    this.size_x = size_x;
  }

  public int getSize_y() {
    return size_y;
  }

  public void setSize_y(int size_y) {
    this.size_y = size_y;
  }

  public String getTitle() {
    return title;
  }

  public void setTitle(String title) {
    this.title = title;
  }

  public URI getSrc() {
    return src;
  }
  
  public void setSrc(URI src) {
    this.src = src;
  }

  public String[] getTokens() {
    return tokens.clone();
  }

  public void setTokens(String[] tokens) {
    this.tokens = tokens.clone();
  }

  public void tokenize() {
    String[] tokens = title.split(" ");
    this.tokens = tokens;
  }
}
