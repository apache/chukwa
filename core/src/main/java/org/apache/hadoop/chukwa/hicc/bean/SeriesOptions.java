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

public class SeriesOptions {
  public boolean show = true;
  public boolean fill = false;
  public int lineWidth;
  public String fillColor;

  public boolean getFill() {
    return fill;
  }

  public void setFill(boolean fill) {
    this.fill = fill;
  }

  public boolean getShow() {
    return show;
  }

  public void setShow(boolean show) {
    this.show = show;
  }

  public int getLineWidth() {
    return lineWidth;
  }

  public void setLineWidth(int lineWidth) {
    this.lineWidth = lineWidth;
  }

  public String getFillColor() {
    return fillColor;
  }

  public void setFillColor(String fillColor) {
    this.fillColor = fillColor;
  }
}
