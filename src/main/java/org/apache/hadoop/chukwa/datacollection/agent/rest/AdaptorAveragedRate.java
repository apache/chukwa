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
package org.apache.hadoop.chukwa.datacollection.agent.rest;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class AdaptorAveragedRate {
  private double rate;
  private long unit;
  
  public AdaptorAveragedRate() {
    rate = 0;
    unit = 0;
  }
  
  public AdaptorAveragedRate(long unit, double rate) {
    this.unit = unit;
    this.rate = rate;
  }

  @XmlAttribute
  public double getRate() {
    return rate;
  }
  
  public void setRate(double rate) {
    this.rate = rate;
  }

  @XmlAttribute
  public long getIntervalInSeconds() {
    return unit;
  }
  
  public void setIntervalInSeconds(long unit) {
    this.unit = unit;
  }
}
