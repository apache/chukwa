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
package org.apache.hadoop.chukwa.rest.bean;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class ClientTraceBean {
  
  private String action;
  private long size = 0L;
  private String src;
  private String dest;
  private String date;
  
  @XmlElement
  public String getAction() {
    return action;
  }
  
  public void setAction(String action) {
    this.action = action;
  }
  
  @XmlElement
  public String getSrc() {
    return src;
  }
  
  public void setSrc(String src) {
    this.src = src;
  }
  
  @XmlElement
  public String getDest() {
    return dest;
  }
  
  public void setDest(String dest) {
    this.dest = dest;
  }
  
  @XmlElement
  public long getSize() {
    return size;
  }
  
  public void setSize(long size) {
    this.size=size;
  }

  @XmlElement
  public String getDate() {
    return date;
  }
  
  public void setDate(String date) {
    this.date = date;    
  }

}
