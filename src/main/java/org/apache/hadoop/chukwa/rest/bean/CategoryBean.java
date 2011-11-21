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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@XmlType(propOrder={"type", "label", "children"})
public class CategoryBean {
  private static Log log = LogFactory.getLog(CategoryBean.class);
  private List<CategoryBean> children = new ArrayList<CategoryBean>();
  private String type = "text";
  private String label = null;
  
  public CategoryBean() {
  }
  
  @XmlElement
  public String getType() {
    return type;
  }
  
  @XmlElement
  public String getLabel() {
    return label;
  }
  
  @XmlElement
  public List<CategoryBean> getChildren() {
    return children;
  }
  
  public void setType(String type) {
    this.type = type;  
  }
  
  public void setLabel(String label) {
    this.label = label;
  }
  
  public void setChildren(List<CategoryBean> children) {
    this.children = children;
  }  
}
