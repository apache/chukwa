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


public class BarOptions extends SeriesOptions {
  public boolean zero;
  public boolean stepByStep = true;
  public int barWidth = 4;
  public String align;
  public boolean horizontal;
  
  public BarOptions() {
    fill = true;
  }
  
  public boolean getZero() {
    return zero;
  }
  
  public void setZero(boolean zero) {
    this.zero = zero;
  }
  
  public boolean getStepByStep() {
    return stepByStep;
  }
  
  public void setStepByStep(boolean stepByStep) {
    this.stepByStep = stepByStep;
  }
  
  public int getBarWidth() {
    return barWidth;
  }
  
  public void setBarWidth(int barWidth) {
    this.barWidth = barWidth;
  }
  
  public String getAlign() {
    return align;
  }
  
  public void setAlign(String align) {
    this.align = align;
  }
  
  public boolean getHorizontal() {
    return this.horizontal;
  }
  
  public void setHorizontal(boolean horizontal) {
    this.horizontal = horizontal;
  }
}
