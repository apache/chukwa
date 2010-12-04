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

import java.text.ParseException;

import javax.xml.bind.annotation.XmlElement;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.simple.JSONObject;

import org.apache.hadoop.chukwa.util.ExceptionUtil;

public class OptionBean {
  private String label;
  private String value;
  private static Log log = LogFactory.getLog(OptionBean.class);

  public OptionBean() {  
  }
  
  public OptionBean(JSONObject json) throws ParseException {
    try {
      label = (String) json.get("label");
      value = (String) json.get("value");
    } catch (Exception e) {
      throw new ParseException(ExceptionUtil.getStackTrace(e), 0);
    }
  }

  @XmlElement
  public String getLabel() {
    return label;
  }
  
  @XmlElement
  public String getValue() {
    return value;
  }
  
  public void setLabel(String label) {
    this.label=label;
  }
  
  public void setValue(String value) {
    this.value=value;
  }
  
  public void update() {
    
  }
  
  public JSONObject deserialize() {
    JSONObject json = new JSONObject();
    try {
      json.put("label", label);
      json.put("value", value);
    } catch (Exception e) {
      log.error(ExceptionUtil.getStackTrace(e));
    }
    return json;
  }
}
