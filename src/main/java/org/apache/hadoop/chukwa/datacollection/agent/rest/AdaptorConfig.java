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

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAccessType;

@XmlRootElement
@XmlAccessorType(XmlAccessType.PUBLIC_MEMBER)
public class AdaptorConfig {
  private String id;
  private String dataType;
  private String adaptorClass;
  private String adaptorParams;
  private long offset;
  
  public AdaptorConfig() {
  }

  @XmlElement
  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  @XmlElement
  public String getDataType() {
    return dataType;
  }
  
  public void setDataType(String dataType) {
    this.dataType = dataType;
  }
  
  @XmlElement  
  public String getAdaptorClass() {
    return adaptorClass;
  }
  
  public void setAdaptorClass(String adaptorClass) {
    this.adaptorClass = adaptorClass;
  }
  
  @XmlElement
  public String getAdaptorParams() {
    return adaptorParams;
  }
  
  public void setAdaptorParams(String adaptorParams) {
    this.adaptorParams = adaptorParams;
  }

  @XmlElement
  public long getOffset() {
    return offset;
  }
  
  public void setOffset(long offset) {
    this.offset = offset;
  }
}
